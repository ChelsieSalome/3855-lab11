import connexion
from connexion import NoContent
import uuid
import yaml
import logging
import logging.config
import json
import datetime
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

with open('/config/receiver_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open("/config/receiver_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

logging.getLogger('kafka').setLevel(logging.WARNING)

logger.info("Configuration loaded - Kafka DEBUG logs suppressed")

KAFKA_SERVER = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
KAFKA_TOPIC = app_config['events']['topic']


# ==========================================
# ISSUE 2 & 3 FIX: Global Kafka Producer
# ==========================================
# Instead of creating a new producer for each event (which is slow),
# we create ONE producer at startup and reuse it for all events.
# This also includes retry logic - if Kafka is unavailable,
# the producer will try to reconnect automatically.

class KafkaProducerWrapper:
    """
    Wrapper around Kafka producer that handles connection failures gracefully.
    
    FIX FOR ISSUE 2: Reusing a single producer instead of creating new ones
    FIX FOR ISSUE 3: Automatic reconnection with exponential backoff
    """
    
    def __init__(self, kafka_server, max_retries=10):
        """
        Initialize the producer wrapper.
        
        Args:
            kafka_server: Kafka server address (hostname:port)
            max_retries: Maximum number of connection attempts before giving up
        """
        self.kafka_server = kafka_server
        self.max_retries = max_retries
        self.producer = None
        self._connect()
    
    def _connect(self):
        """
        Connect to Kafka with retry logic and exponential backoff.
        
        Strategy: Infinite retry with backoff because:
        - Kafka is essential; if it's not available, we MUST wait
        - Exponential backoff prevents hammering Kafka with connection attempts
        - Service will eventually start when Kafka becomes available
        """
        attempt = 1
        
        while True:
            try:
                logger.info(f"[Producer] Connection attempt {attempt}/{self.max_retries} to Kafka at {self.kafka_server}")
                
                # Create producer with timeout settings
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_server,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3,  # Retry individual sends up to 3 times
                    max_in_flight_requests_per_connection=1  # Ensure ordered delivery
                )
                
                logger.info(f"[Producer] ✅ Successfully connected to Kafka at {self.kafka_server}")
                return  # Connection successful, exit the retry loop
                
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[Producer] ❌ Failed to connect (attempt {attempt}/{self.max_retries}): {e}")
                
                if attempt >= self.max_retries:
                    logger.error(f"[Producer] Could not connect after {self.max_retries} attempts. Giving up.")
                    raise Exception(f"Kafka connection failed after {self.max_retries} retries")
                
                # Calculate backoff: 1s, 2s, 4s, 8s, etc. (exponential backoff)
                wait_time = min(2 ** (attempt - 1), 32)  # Cap at 32 seconds
                logger.info(f"[Producer] Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                attempt += 1
    
    def send_message(self, topic, message):
        """
        Send a message to Kafka. If connection fails, automatically reconnect.
        
        Args:
            topic: Kafka topic name
            message: Message to send (will be JSON serialized)
            
        Returns:
            True if successful, False otherwise
        """
        if self.producer is None:
            logger.error("[Producer] Producer not initialized")
            return False
        
        try:
            # Send message and wait for it to complete
            future = self.producer.send(topic, value=message)
            future.get(timeout=10)  # Wait up to 10 seconds for send to complete
            self.producer.flush()  # Ensure it's written
            return True
            
        except KafkaException as e:
            logger.error(f"[Producer] Kafka error when sending message: {e}")
            logger.warning("[Producer] Attempting to reconnect...")
            try:
                self._connect()  # Try to reconnect
            except Exception as reconnect_error:
                logger.error(f"[Producer] Reconnection failed: {reconnect_error}")
            return False
        
        except Exception as e:
            logger.error(f"[Producer] Unexpected error sending message: {e}")
            return False


# Create the global producer at module startup
# This is created ONCE when the app starts, not for each request
try:
    producer_wrapper = KafkaProducerWrapper(KAFKA_SERVER)
    logger.info("✅ Global Kafka producer created successfully")
except Exception as e:
    logger.error(f"❌ Failed to create global Kafka producer: {e}")
    producer_wrapper = None


def report_performance_metrics(body):
    """
    Receive performance metrics and send to Kafka.
    
    FIX FOR ISSUE 2: Uses global producer instead of creating a new one per request
    FIX FOR ISSUE 3: Uses producer wrapper that handles reconnection
    """
    try:
        server_id = body['server_id']
        reporting_timestamp = body['reporting_timestamp']
        metrics = body['metrics']

        for metric in metrics:
            trace_id = str(uuid.uuid4())

            logger.info(f"RECEIVED: Performance metric from server {server_id} (trace: {trace_id})")

            individual_event = {
                "trace_id": trace_id,
                "server_id": server_id,
                "cpu": metric['cpu'],
                "memory": metric['memory'],
                "disk_io": metric['disk_io'],
                "reporting_timestamp": reporting_timestamp
            }

            msg = {
                "type": "performance_metric",
                "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload": individual_event
            }

            # FIX: Use global producer instead of creating new one
            if producer_wrapper and producer_wrapper.send_message(KAFKA_TOPIC, msg):
                logger.info(f"SENT TO KAFKA: Performance metric (trace: {trace_id})")
            else:
                logger.error(f"FAILED TO SEND: Performance metric (trace: {trace_id})")
                return {"error": "Failed to send to Kafka"}, 500

        return NoContent, 201
        
    except Exception as e:
        logger.error(f"Error processing performance metrics: {e}")
        return {"error": str(e)}, 500


def report_error_metrics(body):
    """
    Receive error metrics and send to Kafka.
    
    FIX FOR ISSUE 2: Uses global producer instead of creating a new one per request
    FIX FOR ISSUE 3: Uses producer wrapper that handles reconnection
    """
    try:
        server_id = body['server_id']
        reporting_timestamp = body['reporting_timestamp']
        errors = body['errors']

        logger.info(f"RECEIVED: Error metrics from server {server_id}")

        if not errors:
            return NoContent, 201

        for error in errors:
            trace_id = str(uuid.uuid4())

            individual_event = {
                "trace_id": trace_id,
                "server_id": server_id,
                "error_code": error['error_code'],
                "severity_level": error['severity_level'],
                "avg_response_time": error['avg_response_time'],
                "error_message": error.get('error_message', ''),
                "reporting_timestamp": reporting_timestamp
            }

            msg = {
                "type": "error_metric",
                "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload": individual_event
            }

            # FIX: Use global producer instead of creating new one
            if producer_wrapper and producer_wrapper.send_message(KAFKA_TOPIC, msg):
                logger.info(f"SENT TO KAFKA: Error metric (trace: {trace_id}, code: {error['error_code']})")
            else:
                logger.error(f"FAILED TO SEND: Error metric (trace: {trace_id})")
                return {"error": "Failed to send to Kafka"}, 500

        return NoContent, 201
        
    except Exception as e:
        logger.error(f"Error processing error metrics: {e}")
        return {"error": str(e)}, 500


def health():
    """Health check endpoint"""
    return {"status": "healthy"}, 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("receiver_openapi.yaml",
            strict_validation=True,
            validate_responses=True)

flask_app = app.app


@flask_app.route('/')
def home():
    """Home page with links to API endpoints"""
    return '''
    <html>
    <head><title>Receiver Service</title></head>
    <body>
        <h1>Receiver Service</h1>
        <h2>Available Endpoints:</h2>
        <ul>
            <li>POST /monitoring/performance</li>
            <li>POST /monitoring/errors</li>
        </ul>
    </body>
    </html>
    '''


if __name__ == "__main__":
    logger.info("Starting Receiver Service on port 8080")
    app.run(host='0.0.0.0', port=8080)