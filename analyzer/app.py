"""
Analyzer Service - app.py
Reads events directly from Kafka (from the beginning) to answer analytics queries.
Lab 11 fixes:
  - Single persistent KafkaConsumer (created once, reused for all requests)
  - Retry logic with exponential backoff
  - Thread lock so concurrent requests don't corrupt the consumer state
"""

import connexion
from connexion import FlaskApp
from flask_cors import CORS
import json
import logging
import logging.config
import yaml
import time
import threading
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

# ── Config & Logging ──────────────────────────────────────────────────────────

with open('/config/analyzer_config.yml', 'r') as f:
    CONFIG = yaml.safe_load(f)

with open('/config/analyzer_log_config.yml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f))

logger = logging.getLogger('basicLogger')

logging.getLogger('kafka').setLevel(logging.WARNING)

KAFKA_SERVER = f"{CONFIG['kafka']['hostname']}:{CONFIG['kafka']['port']}"
KAFKA_TOPIC  = CONFIG['kafka']['topic']

# ── Global Kafka Consumer ─────────────────────────────────────────────────────

_consumer_lock = threading.Lock()
_consumer: KafkaConsumer | None = None
_consumer_initialized = False          # True after the first seek_to_beginning


def _create_consumer() -> KafkaConsumer:
    """
    Keep retrying with exponential backoff until Kafka is reachable.
    Returns a ready KafkaConsumer.
    """
    delay = 1
    attempt = 0
    while True:
        attempt += 1
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                group_id='analyzer_group',
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000,   # stop iteration when no more messages
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            )
            logger.info(f"Kafka consumer connected on attempt {attempt}")
            return consumer
        except (NoBrokersAvailable, KafkaError) as e:
            logger.warning(
                f"Kafka not available (attempt {attempt}): {e}. Retrying in {delay}s …"
            )
            time.sleep(delay)
            delay = min(delay * 2, 32)
        except Exception as e:
            logger.error(f"Unexpected error creating consumer: {e}. Retrying in {delay}s …")
            time.sleep(delay)
            delay = min(delay * 2, 32)


def _get_consumer() -> KafkaConsumer:
    """Return the global consumer, creating it if needed."""
    global _consumer, _consumer_initialized
    if _consumer is None:
        _consumer = _create_consumer()
        # First-time init: do a dummy poll so partition assignment happens,
        # then seek to the very beginning so we can count from offset 0.
        _consumer.poll(timeout_ms=1000)
        _consumer.seek_to_beginning()
        _consumer_initialized = True
    return _consumer


def _read_all_messages() -> list[dict]:
    """
    Seek to beginning and drain all current messages.
    Must be called with _consumer_lock held.
    Returns a list of raw message value dicts.
    """
    global _consumer, _consumer_initialized
    consumer = _get_consumer()

    try:
        consumer.seek_to_beginning()
    except Exception as e:
        logger.error(f"seek_to_beginning failed: {e}. Recreating consumer …")
        try:
            consumer.close()
        except Exception:
            pass
        _consumer = None
        _consumer_initialized = False
        consumer = _get_consumer()
        consumer.seek_to_beginning()

    messages = []
    for msg in consumer:            # stops when consumer_timeout_ms is hit
        messages.append(msg.value)
    return messages


# ── API Endpoints ─────────────────────────────────────────────────────────────

def get_performance_event(index: int):
    """GET /analyzer/performance?index=N"""
    logger.info(f"Request for performance event at index {index}")

    with _consumer_lock:
        try:
            messages = _read_all_messages()
        except Exception as e:
            logger.error(f"Error reading Kafka: {e}")
            return {"message": f"Error reading Kafka: {e}"}, 400

    perf_events = [m['payload'] for m in messages if m.get('type') == 'performance_metric']

    if index >= len(perf_events):
        logger.error(f"No performance event at index {index} (total={len(perf_events)})")
        return {"message": f"No performance event at index {index}"}, 404

    logger.info(f"Returning performance event at index {index}")
    return perf_events[index], 200


def get_error_event(index: int):
    """GET /analyzer/error?index=N"""
    logger.info(f"Request for error event at index {index}")

    with _consumer_lock:
        try:
            messages = _read_all_messages()
        except Exception as e:
            logger.error(f"Error reading Kafka: {e}")
            return {"message": f"Error reading Kafka: {e}"}, 400

    error_events = [m['payload'] for m in messages if m.get('type') == 'error_metric']

    if index >= len(error_events):
        logger.error(f"No error event at index {index} (total={len(error_events)})")
        return {"message": f"No error event at index {index}"}, 404

    logger.info(f"Returning error event at index {index}")
    return error_events[index], 200


def get_stats():
    """GET /analyzer/stats"""
    logger.info("Request for event statistics")

    with _consumer_lock:
        try:
            messages = _read_all_messages()
        except Exception as e:
            logger.error(f"Error reading Kafka: {e}")
            return {"message": f"Error reading Kafka: {e}"}, 400

    perf_count  = sum(1 for m in messages if m.get('type') == 'performance_metric')
    error_count = sum(1 for m in messages if m.get('type') == 'error_metric')

    stats = {
        "num_performance_events": perf_count,
        "num_error_events":       error_count,
    }
    logger.info(f"Stats: {stats}")
    return stats, 200


def health():
    """GET /analyzer/health – liveness probe."""
    return {"status": "healthy"}, 200


# ── App Setup ─────────────────────────────────────────────────────────────────

app = FlaskApp(__name__, specification_dir='')
CORS(app.app)

app.add_api(
    'openapi.yaml',
    base_path='/analyzer',
    strict_validation=True,
    validate_responses=True,
)

if __name__ == '__main__':
    logger.info("Starting Analyzer Service on port 5005")
    app.run(host='0.0.0.0', port=CONFIG['app']['port'])