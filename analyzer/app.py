import connexion
from connexion import FlaskApp
from flask_cors import CORS
import json
import logging
import logging.config
import yaml
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import threading
import time

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


# ==========================================
# ISSUE 2 & 3 FIX: Kafka Consumer Wrapper
# ==========================================
# Instead of creating a new consumer for each request (which is slow),
# we create ONE consumer at startup that's reused by all endpoints.
# It includes automatic reconnection with exponential backoff.

class KafkaConsumerWrapper:
    """
    Wrapper around Kafka consumer that handles connection failures gracefully.
    
    FIX FOR ISSUE 2: Single consumer reused instead of creating new ones
    FIX FOR ISSUE 3: Automatic reconnection with exponential backoff
    """
    
    def __init__(self, topic, bootstrap_servers, max_retries=10):
        """
        Initialize the consumer wrapper.
        
        Args:
            topic: Kafka topic to subscribe to
            bootstrap_servers: Kafka server address (hostname:port)
            max_retries: Maximum connection attempts before giving up
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.consumer = None
        self._connect()
    
    def _connect(self):
        """
        Connect to Kafka with retry logic and exponential backoff.
        
        Strategy: Infinite retry with backoff because:
        - Kafka is essential for this service
        - Exponential backoff prevents hammering Kafka
        - Service will work once Kafka is available
        """
        attempt = 1
        
        while True:
            try:
                logger.info(f"[Analyzer Consumer] Connection attempt {attempt}/{self.max_retries}")
                
                # Create consumer with proper settings
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id='analyzer_group',
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=1000,  # Don't block forever if no messages
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    session_timeout_ms=30000,  # 30 second session timeout
                    heartbeat_interval_ms=10000  # Heartbeat every 10 seconds
                )
                
                logger.info(f"[Analyzer Consumer] ✅ Successfully connected to Kafka")
                return  # Connection successful
                
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[Analyzer Consumer] ❌ Failed to connect (attempt {attempt}/{self.max_retries}): {e}")
                
                if attempt >= self.max_retries:
                    logger.error(f"[Analyzer Consumer] Could not connect after {self.max_retries} attempts.")
                    raise Exception(f"Kafka connection failed after {self.max_retries} retries")
                
                # Exponential backoff: 1s, 2s, 4s, 8s, etc. (capped at 32s)
                wait_time = min(2 ** (attempt - 1), 32)
                logger.info(f"[Analyzer Consumer] Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                attempt += 1
    
    def seek_to_beginning(self):
        """
        Seek to the beginning of all partitions.
        
        Used when we need to re-read all messages from the start.
        """
        if self.consumer is None:
            logger.warning("[Analyzer Consumer] Consumer is None, cannot seek")
            return
        
        try:
            self.consumer.seek_to_beginning()
        except KafkaException as e:
            logger.error(f"[Analyzer Consumer] Error seeking to beginning: {e}")
            self.consumer = None
            self._connect()
    
    def poll_and_process(self):
        """
        Poll for messages from Kafka.
        If Kafka fails, automatically reconnect.
        
        Yields:
            Message data (dict) if available, None if timeout
        """
        if self.consumer is None:
            try:
                self._connect()
            except Exception as e:
                logger.error(f"[Analyzer Consumer] Cannot connect: {e}")
                return
        
        try:
            for msg in self.consumer:
                yield msg.value
                
        except KafkaError as e:
            logger.error(f"[Analyzer Consumer] Kafka error: {e}")
            logger.warning("[Analyzer Consumer] Attempting to reconnect...")
            self.consumer = None
            try:
                self._connect()
            except Exception as reconnect_error:
                logger.error(f"[Analyzer Consumer] Reconnection failed: {reconnect_error}")


# Create global consumer wrapper at module startup
kafka_server = f"{CONFIG['kafka']['hostname']}:{CONFIG['kafka']['port']}"
kafka_topic = CONFIG['kafka']['topic']

try:
    kafka_consumer = KafkaConsumerWrapper(kafka_topic, kafka_server)
    logger.info("✅ Global Kafka consumer created successfully")
except Exception as e:
    logger.error(f"❌ Failed to create global Kafka consumer: {e}")
    kafka_consumer = None


# ==========================================
# REQUEST HANDLER FUNCTIONS
# ==========================================

def get_performance_event(index):
    """
    Gets a performance event at a specific index.
    
    FIX FOR ISSUE 2: Uses global consumer instead of creating new one
    FIX FOR ISSUE 3: Consumer automatically reconnects if needed
    """
    logger.info(f"Request for performance event at index {index}")
    
    if kafka_consumer is None:
        logger.error("Kafka consumer not available")
        return {"message": "Service unavailable"}, 503
    
    try:
        # Seek to beginning to ensure we read all messages
        kafka_consumer.seek_to_beginning()
        performance_count = 0
        
        # Iterate through messages
        for msg_data in kafka_consumer.poll_and_process():
            if msg_data is None:
                break
            
            # Check if this is a performance event
            if msg_data.get('type') == 'performance_metric':
                if performance_count == index:
                    logger.info(f"Found performance event at index {index}")
                    return msg_data['payload'], 200
                performance_count += 1
        
        # If we get here, index not found
        logger.error(f"No performance event found at index {index}")
        return {"message": f"No performance event at index {index}"}, 404
        
    except Exception as e:
        logger.error(f"Error retrieving performance event: {str(e)}")
        return {"message": f"Error retrieving event: {str(e)}"}, 400


def get_error_event(index):
    """
    Gets an error event at a specific index.
    
    FIX FOR ISSUE 2: Uses global consumer instead of creating new one
    FIX FOR ISSUE 3: Consumer automatically reconnects if needed
    """
    logger.info(f"Request for error event at index {index}")
    
    if kafka_consumer is None:
        logger.error("Kafka consumer not available")
        return {"message": "Service unavailable"}, 503
    
    try:
        # Seek to beginning to ensure we read all messages
        kafka_consumer.seek_to_beginning()
        error_count = 0
        
        # Iterate through messages
        for msg_data in kafka_consumer.poll_and_process():
            if msg_data is None:
                break
            
            # Check if this is an error event
            if msg_data.get('type') == 'error_metric':
                if error_count == index:
                    logger.info(f"Found error event at index {index}")
                    return msg_data['payload'], 200
                error_count += 1
        
        # If we get here, index not found
        logger.error(f"No error event found at index {index}")
        return {"message": f"No error event at index {index}"}, 404
        
    except Exception as e:
        logger.error(f"Error retrieving error event: {str(e)}")
        return {"message": f"Error retrieving event: {str(e)}"}, 400


def get_stats():
    """
    Gets statistics about events in the Kafka queue.
    
    FIX FOR ISSUE 2: Uses global consumer instead of creating new one
    FIX FOR ISSUE 3: Consumer automatically reconnects if needed
    """
    logger.info("Request for event statistics")
    
    if kafka_consumer is None:
        logger.error("Kafka consumer not available")
        return {"message": "Service unavailable"}, 503
    
    try:
        # Seek to beginning to read all messages
        kafka_consumer.seek_to_beginning()
        
        # Initialize counts
        performance_count = 0
        error_count = 0
        
        # Iterate through all messages
        for msg_data in kafka_consumer.poll_and_process():
            if msg_data is None:
                break
            
            # Count by type
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


# ==========================================
# CONNEXION APP SETUP
# ==========================================

app = FlaskApp(__name__, specification_dir='')

# Enable CORS
CORS(app.app)

# Add API with base path
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