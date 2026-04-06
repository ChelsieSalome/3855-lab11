import connexion
from connexion import FlaskApp
from flask_cors import CORS
import json
import logging
import logging.config
import yaml
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
import threading

# Load configuration
with open('/config/analyzer_config.yml', 'r') as f:
    CONFIG = yaml.safe_load(f)

# Load logging configuration
with open('/config/analyzer_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Suppress Kafka DEBUG logs
logging.getLogger('kafka').setLevel(logging.WARNING)

logger.info("Analyzer service configuration loaded")


class KafkaConsumerWrapper:
    """
    Thread-safe wrapper around Kafka consumer.
    Uses a lock to prevent concurrent access issues.
    """
    
    def __init__(self, topic, bootstrap_servers, max_retries=10):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.consumer = None
        self.lock = threading.RLock()  # Reentrant lock for thread safety
        self._connect()
    
    def _connect(self):
        """Connect to Kafka with retry logic and exponential backoff."""
        attempt = 1
        while True:
            try:
                logger.info(f"[Analyzer] Connection attempt {attempt}/{self.max_retries}")
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id='analyzer_group',
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                logger.info(f"[Analyzer] ✅ Successfully connected to Kafka")
                return
                
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[Analyzer] ❌ Failed to connect (attempt {attempt}/{self.max_retries}): {e}")
                if attempt >= self.max_retries:
                    logger.error(f"[Analyzer] Could not connect after {self.max_retries} attempts.")
                    raise Exception(f"Kafka connection failed after {self.max_retries} retries")
                
                wait_time = min(2 ** (attempt - 1), 32)
                logger.info(f"[Analyzer] Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                attempt += 1
    
    def get_all_messages(self):
        """
        Read all messages from Kafka starting from the beginning.
        Thread-safe - uses lock to prevent concurrent access.
        
        CRITICAL: Does NOT use generators (causes "generator already executing" error).
        Instead, reads all messages into a list under a lock.
        """
        with self.lock:  # Acquire lock - only one request can do this at a time
            if self.consumer is None:
                try:
                    self._connect()
                except Exception as e:
                    logger.error(f"[Analyzer] Cannot connect: {e}")
                    return []
            
            try:
                # CRITICAL FIX: Poll FIRST to trigger partition assignment
                logger.debug("[Analyzer] Polling to trigger partition assignment...")
                self.consumer.poll(timeout_ms=100)
                
                # NOW seek to beginning - partitions are assigned
                logger.debug("[Analyzer] Seeking to beginning...")
                self.consumer.seek_to_beginning()
                
                # Read ALL messages into a list (not a generator)
                # This prevents "generator already executing" errors
                messages = []
                for msg in self.consumer:
                    if msg is None:
                        break
                    messages.append(msg.value)
                
                logger.debug(f"[Analyzer] Read {len(messages)} messages from Kafka")
                return messages
                    
            except KafkaError as e:
                logger.error(f"[Analyzer] Kafka error: {e}")
                logger.warning("[Analyzer] Attempting to reconnect...")
                self.consumer = None
                try:
                    self._connect()
                except Exception as reconnect_error:
                    logger.error(f"[Analyzer] Reconnection failed: {reconnect_error}")
                return []


# Create global consumer at module startup
kafka_server = f"{CONFIG['kafka']['hostname']}:{CONFIG['kafka']['port']}"
kafka_topic = CONFIG['kafka']['topic']

try:
    kafka_consumer = KafkaConsumerWrapper(kafka_topic, kafka_server)
    logger.info("✅ Global Kafka consumer created successfully")
except Exception as e:
    logger.error(f"❌ Failed to create global Kafka consumer: {e}")
    kafka_consumer = None


def get_performance_event(index):
    """Gets a performance event at a specific index."""
    logger.info(f"Request for performance event at index {index}")
    
    if kafka_consumer is None:
        logger.error("Kafka consumer not available")
        return {"message": "Service unavailable"}, 503
    
    try:
        # Get all messages (thread-safe, no generators)
        all_messages = kafka_consumer.get_all_messages()
        
        performance_count = 0
        for msg_data in all_messages:
            if msg_data.get('type') == 'performance_metric':
                if performance_count == index:
                    logger.info(f"Found performance event at index {index}")
                    return msg_data['payload'], 200
                performance_count += 1
        
        logger.error(f"No performance event found at index {index}")
        return {"message": f"No performance event at index {index}"}, 404
        
    except Exception as e:
        logger.error(f"Error retrieving performance event: {str(e)}")
        return {"message": f"Error retrieving event: {str(e)}"}, 400


def get_error_event(index):
    """Gets an error event at a specific index."""
    logger.info(f"Request for error event at index {index}")
    
    if kafka_consumer is None:
        logger.error("Kafka consumer not available")
        return {"message": "Service unavailable"}, 503
    
    try:
        # Get all messages (thread-safe, no generators)
        all_messages = kafka_consumer.get_all_messages()
        
        error_count = 0
        for msg_data in all_messages:
            if msg_data.get('type') == 'error_metric':
                if error_count == index:
                    logger.info(f"Found error event at index {index}")
                    return msg_data['payload'], 200
                error_count += 1
        
        logger.error(f"No error event found at index {index}")
        return {"message": f"No error event at index {index}"}, 404
        
    except Exception as e:
        logger.error(f"Error retrieving error event: {str(e)}")
        return {"message": f"Error retrieving event: {str(e)}"}, 400


def get_stats():
    """Gets statistics about events in the Kafka queue."""
    logger.info("Request for event statistics")
    
    if kafka_consumer is None:
        logger.error("Kafka consumer not available")
        return {"message": "Service unavailable"}, 503
    
    try:
        # Get all messages (thread-safe, no generators)
        all_messages = kafka_consumer.get_all_messages()
        
        performance_count = 0
        error_count = 0
        
        for msg_data in all_messages:
            if msg_data.get('type') == 'performance_metric':
                performance_count += 1
            elif msg_data.get('type') == 'error_metric':
                error_count += 1
        
        stats = {
            "num_performance_events": performance_count,
            "num_error_events": error_count
        }
        
        logger.info(f"Statistics: {stats}")
        return stats, 200
        
    except Exception as e:
        logger.error(f"Error retrieving statistics: {str(e)}")
        return {"message": f"Error retrieving statistics: {str(e)}"}, 400


def health():
    """Health check endpoint"""
    return {"status": "healthy"}, 200


app = FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.add_api(
    'openapi.yaml',
    base_path='/analyzer',
    strict_validation=True,
    validate_responses=True
)

if __name__ == '__main__':
    logger.info("Starting Analyzer Service on port 5005")
    app.run(
        host='0.0.0.0',
        port=CONFIG['app']['port']
    )