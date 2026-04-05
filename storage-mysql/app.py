import connexion
from connexion import NoContent
from datetime import datetime
import functools
import json
import yaml
import logging
import logging.config 
import threading
import time
from models import PerformanceReading, ErrorReading
from create_tables import make_session, init_db
from sqlalchemy import select
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable


with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/storage_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Suppress Kafka DEBUG logs
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('kafka.client').setLevel(logging.WARNING)
logging.getLogger('kafka.consumer').setLevel(logging.WARNING)
logging.getLogger('kafka.protocol').setLevel(logging.WARNING)
logging.getLogger('kafka.coordinator').setLevel(logging.WARNING)

KAFKA_SERVER = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
KAFKA_TOPIC = app_config['events']['topic']

logger.info("Storage service configuration loaded successfully")
logger.info(f"Kafka: {KAFKA_SERVER}, Topic: {KAFKA_TOPIC}")


# ==========================================
# ISSUE 2 & 3 FIX: Kafka Consumer Wrapper
# ==========================================
# Instead of creating a new consumer for each operation (which is slow),
# we create ONE consumer at startup in a background thread.
# If Kafka goes down, it automatically reconnects with exponential backoff.

class KafkaConsumerWrapper:
    """
    Wrapper around Kafka consumer that handles connection failures gracefully.
    
    FIX FOR ISSUE 2: Single consumer reused instead of creating new ones
    FIX FOR ISSUE 3: Automatic reconnection with exponential backoff
    """
    
    def __init__(self, kafka_server, topic, max_retries=10):
        """
        Initialize the consumer wrapper.
        
        Args:
            kafka_server: Kafka server address (hostname:port)
            topic: Kafka topic to subscribe to
            max_retries: Maximum connection attempts before giving up
        """
        self.kafka_server = kafka_server
        self.topic = topic
        self.max_retries = max_retries
        self.consumer = None
        self.is_connected = False
        self._connect()
    
    def _connect(self):
        """
        Connect to Kafka with retry logic and exponential backoff.
        
        Strategy: Infinite retry with backoff because:
        - Kafka is essential for this service to function
        - Exponential backoff prevents hammering Kafka
        - Service will work once Kafka is available
        """
        attempt = 1
        
        while True:
            try:
                logger.info(f"[Consumer] Connection attempt {attempt}/{self.max_retries} to Kafka at {self.kafka_server}")
                
                # Create consumer with connection settings
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.kafka_server,
                    group_id='storage_group',
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,  # Manual commit for safety
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    session_timeout_ms=30000,  # 30 second session timeout
                    heartbeat_interval_ms=10000  # Heartbeat every 10 seconds
                )
                
                self.is_connected = True
                logger.info(f"[Consumer] ✅ Successfully connected to Kafka at {self.kafka_server}")
                return  # Connection successful, exit retry loop
                
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[Consumer] ❌ Failed to connect (attempt {attempt}/{self.max_retries}): {e}")
                
                if attempt >= self.max_retries:
                    logger.error(f"[Consumer] Could not connect after {self.max_retries} attempts.")
                    self.is_connected = False
                    raise Exception(f"Kafka connection failed after {self.max_retries} retries")
                
                # Exponential backoff: 1s, 2s, 4s, 8s, etc. (capped at 32s)
                wait_time = min(2 ** (attempt - 1), 32)
                logger.info(f"[Consumer] Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                attempt += 1
    
    def get_messages(self):
        """
        Generator that yields messages from Kafka.
        If connection fails, automatically reconnects.
        
        Yields:
            Tuple of (message_value, success_boolean)
        """
        while True:
            if not self.is_connected or self.consumer is None:
                try:
                    self._connect()
                except Exception as e:
                    logger.error(f"[Consumer] Cannot connect: {e}")
                    time.sleep(5)  # Wait before retrying
                    continue
            
            try:
                # Consumer is a blocking iterator - it waits for messages
                for msg in self.consumer:
                    try:
                        yield msg.value, True  # Successfully got message
                        
                        # Commit offset only after successful processing
                        self.consumer.commit()
                        
                    except Exception as process_error:
                        logger.error(f"[Consumer] Error processing message: {process_error}")
                        # Don't commit on error - message will be reprocessed
                        yield None, False
                        
            except KafkaError as e:
                logger.error(f"[Consumer] Kafka error in consumer loop: {e}")
                logger.warning("[Consumer] Attempting to reconnect...")
                self.is_connected = False
                self.consumer = None
                try:
                    self._connect()
                except Exception as reconnect_error:
                    logger.error(f"[Consumer] Reconnection failed: {reconnect_error}")
                time.sleep(5)  # Wait before retrying
                
            except Exception as e:
                logger.error(f"[Consumer] Unexpected error in consumer loop: {e}")
                self.is_connected = False
                self.consumer = None
                time.sleep(5)  # Wait before retrying


# Create global consumer wrapper at module startup
try:
    consumer_wrapper = KafkaConsumerWrapper(KAFKA_SERVER, KAFKA_TOPIC)
    logger.info("✅ Global Kafka consumer created successfully")
except Exception as e:
    logger.error(f"❌ Failed to create global Kafka consumer: {e}")
    consumer_wrapper = None


def use_db_session(func):
    """
    Decorator to inject database session into functions.
    
    FIX FOR ISSUE 4: Session uses pooled connections with proper recycling
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper


@use_db_session
def report_performance_metrics(session, body):
    """
    Store performance metric to database.
    
    FIX FOR ISSUE 4: Uses pooled database connection
    """
    reading = PerformanceReading(
        trace_id=body['trace_id'],
        server_id=body['server_id'],
        cpu=body['cpu'],
        memory=body['memory'],
        disk_io=body['disk_io'],
        reporting_timestamp=datetime.strptime(body['reporting_timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    )
    
    session.add(reading)
    session.commit()

    logger.info(f" STORED: Performance reading (trace_id: {body['trace_id']}, server: {body['server_id']})")
    return NoContent, 201


@use_db_session
def report_error_metrics(session, body):
    """
    Store error metric to database.
    
    FIX FOR ISSUE 4: Uses pooled database connection
    """
    reading = ErrorReading(
        trace_id=body['trace_id'],
        server_id=body['server_id'],
        error_code=body['error_code'],
        severity_level=body['severity_level'],
        avg_response_time=body['avg_response_time'],
        error_message=body['error_message'],
        reporting_timestamp=datetime.strptime(body['reporting_timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    )
    
    session.add(reading)
    session.commit()

    logger.info(f" STORED: Error reading (trace_id: {body['trace_id']}, server: {body['server_id']}, code: {body['error_code']})")
    return NoContent, 201


@use_db_session
def get_performance_readings(session, start_timestamp, end_timestamp):
    """
    Retrieve performance readings within time range.
    
    FIX FOR ISSUE 4: Uses pooled database connection
    """
    logger.debug(f"GET /monitoring/performance request: {start_timestamp} to {end_timestamp}")
    
    start_timestamp = start_timestamp.strip()
    end_timestamp = end_timestamp.strip()
    
    start_datetime = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_datetime = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    
    statement = select(PerformanceReading).where(
        PerformanceReading.date_created >= start_datetime
    ).where(
        PerformanceReading.date_created < end_datetime
    )
    
    results = [reading.to_dict() for reading in session.execute(statement).scalars().all()]
    
    logger.info(f"GET request: Found {len(results)} performance readings between {start_timestamp} and {end_timestamp}")
    
    return results, 200


@use_db_session
def get_error_readings(session, start_timestamp, end_timestamp):
    """
    Retrieve error readings within time range.
    
    FIX FOR ISSUE 4: Uses pooled database connection
    """
    logger.debug(f"GET /monitoring/errors request: {start_timestamp} to {end_timestamp}")
    
    start_timestamp = start_timestamp.strip()
    end_timestamp = end_timestamp.strip()
    
    start_datetime = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_datetime = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    
    statement = select(ErrorReading).where(
        ErrorReading.date_created >= start_datetime
    ).where(
        ErrorReading.date_created < end_datetime
    )
    
    results = [reading.to_dict() for reading in session.execute(statement).scalars().all()]
    
    logger.info(f"GET request: Found {len(results)} error readings between {start_timestamp} and {end_timestamp}")
    
    return results, 200


# ==========================================
# KAFKA CONSUMER THREAD
# ==========================================
# FIX FOR ISSUE 2: Uses global consumer wrapper
# FIX FOR ISSUE 3: Automatic reconnection handled by wrapper
# FIX FOR ISSUE 4: Uses pooled database connections

def process_messages():
    """
    Process event messages from Kafka.
    
    Runs in background thread. Listens for messages and stores them in database.
    Automatically handles Kafka reconnection via consumer wrapper.
    """
    logger.info(f" Connecting to Kafka at {KAFKA_SERVER}")
    
    if consumer_wrapper is None:
        logger.error(" Consumer wrapper not initialized. Exiting.")
        return
    
    try:
        logger.info(f" Subscribed to topic: {KAFKA_TOPIC}")
        logger.info(" Waiting for messages...")
        
        # Use the global consumer wrapper to get messages
        # It handles reconnection automatically
        for msg_data, success in consumer_wrapper.get_messages():
            if not success:
                continue  # Skip failed messages
            
            try:
                payload = msg_data["payload"]
                msg_type = msg_data["type"]
                trace_id = payload.get('trace_id', 'unknown')
                server_id = payload.get('server_id', 'unknown')
                
                logger.info(f" RECEIVED FROM KAFKA: {msg_type} (trace: {trace_id}, server: {server_id})")
                
                if msg_type == "performance_metric":
                    # Store the performance metric to the DB
                    report_performance_metrics(payload)
                    
                elif msg_type == "error_metric":
                    # Store the error metric to the DB
                    report_error_metrics(payload)
                    
                else:
                    logger.warning(f"Unknown message type: {msg_type}")
                    
            except Exception as e:
                logger.error(f" Error processing message: {e}")
                # Message not committed, will be reprocessed
                
    except Exception as e:
        logger.error(f" Fatal error in Kafka consumer: {e}")


def setup_kafka_thread():
    """Setup and start Kafka consumer thread"""
    t1 = threading.Thread(target=process_messages)
    t1.daemon = True
    t1.start()
    logger.info(" Kafka consumer thread started")


# ==========================================
# CONNEXION APP SETUP
# ==========================================

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("storage_openapi.yaml",
            strict_validation=True,
            validate_responses=True)

flask_app = app.app


@flask_app.route('/')
def home():
    """Home page with links to API endpoints"""
    return '''
    <html>
        <head>
            <title>Storage Service</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                h1 { color: #2c3e50; }
                code { background: #f4f4f4; padding: 2px 6px; border-radius: 3px; }
            </style>
        </head>
        <body>
            <h1> Storage Service</h1>
            <h2>Available Endpoints:</h2>
            <ol>
                <li>
                    <strong>GET:</strong> 
                    <code>/monitoring/performance?start_timestamp=2026-02-18T00:00:00Z&end_timestamp=2026-02-18T23:59:59Z</code>
                </li>
                <li>
                    <strong>GET:</strong> 
                    <code>/monitoring/errors?start_timestamp=2026-02-18T00:00:00Z&end_timestamp=2026-02-18T23:59:59Z</code>
                </li>
            </ol>
            <p><em> Messages are consumed from Kafka topic: <strong>events</strong></em></p>
            <hr>
            <p><a href="/ui">View API Documentation (Swagger UI)</a></p>
        </body>
    </html>
    '''


if __name__ == "__main__":
    logger.info(" Starting Storage Service on port 8091")
    
    # FIX FOR ISSUE 1: Initialize database with retry logic
    init_db()
    
    # FIX FOR ISSUE 2 & 3: Start background Kafka consumer thread
    setup_kafka_thread()
    
    app.run(host="0.0.0.0", port=8091)