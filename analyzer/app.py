"""
Analyzer Service - app.py
Lab 11 fix:
  - Messages are cached in memory and refreshed by a background thread every 2s.
  - API endpoints read from the cache instantly — no Kafka seek on every request.
  - /health never touches Kafka, so the health check never times out.
  - Thread lock only protects the cache swap, not a full Kafka read.
"""

import connexion
from connexion import FlaskApp
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
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

# ── In-memory message cache ───────────────────────────────────────────────────
# Background thread writes here; API handlers read from here instantly.

_cache_lock     = threading.Lock()
_cached_messages: list[dict] = []   # list of raw message value dicts


def _create_consumer() -> KafkaConsumer:
    """Retry with exponential backoff until Kafka is reachable."""
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
                consumer_timeout_ms=500,    # stop iterating quickly when caught up
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            )
            logger.info(f"Kafka consumer connected on attempt {attempt}")
            return consumer
        except (NoBrokersAvailable, KafkaError) as e:
            logger.warning(f"Kafka not available (attempt {attempt}): {e}. Retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 32)
        except Exception as e:
            logger.error(f"Unexpected error creating consumer: {e}. Retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 32)


def _refresh_cache():
    """
    Background daemon thread.
    Reads ALL messages from Kafka every 2 seconds and updates the cache.
    Reconnects automatically on failure.
    """
    global _cached_messages
    logger.info("Cache refresh thread started")

    consumer = _create_consumer()

    while True:
        try:
            # Seek to beginning and drain all current messages
            consumer.poll(timeout_ms=100)   # ensure partition assignment
            consumer.seek_to_beginning()

            messages = []
            for msg in consumer:            # stops when consumer_timeout_ms hit
                messages.append(msg.value)

            with _cache_lock:
                _cached_messages = messages

            logger.debug(f"Cache refreshed: {len(messages)} messages")

        except Exception as e:
            logger.error(f"Cache refresh error: {e}. Reconnecting in 5s …")
            try:
                consumer.close()
            except Exception:
                pass
            time.sleep(5)
            consumer = _create_consumer()

        time.sleep(2)   # refresh every 2 seconds


# Start background cache thread immediately at import time
_refresh_thread = threading.Thread(target=_refresh_cache, daemon=True)
_refresh_thread.start()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_cached() -> list[dict]:
    """Return a snapshot of the current cache."""
    with _cache_lock:
        return list(_cached_messages)


# ── API Endpoints ─────────────────────────────────────────────────────────────

def get_performance_event(index: int):
    """GET /analyzer/performance?index=N"""
    logger.info(f"Request for performance event at index {index}")

    messages    = _get_cached()
    perf_events = [m['payload'] for m in messages if m.get('type') == 'performance_metric']

    if index >= len(perf_events):
        logger.error(f"No performance event at index {index} (total={len(perf_events)})")
        return {"message": f"No performance event at index {index}"}, 404

    logger.info(f"Returning performance event at index {index}")
    return perf_events[index], 200


def get_error_event(index: int):
    """GET /analyzer/error?index=N"""
    logger.info(f"Request for error event at index {index}")

    messages     = _get_cached()
    error_events = [m['payload'] for m in messages if m.get('type') == 'error_metric']

    if index >= len(error_events):
        logger.error(f"No error event at index {index} (total={len(error_events)})")
        return {"message": f"No error event at index {index}"}, 404

    logger.info(f"Returning error event at index {index}")
    return error_events[index], 200


def get_stats():
    """GET /analyzer/stats"""
    logger.info("Request for event statistics")

    messages    = _get_cached()
    perf_count  = sum(1 for m in messages if m.get('type') == 'performance_metric')
    error_count = sum(1 for m in messages if m.get('type') == 'error_metric')

    stats = {
        "num_performance_events": perf_count,
        "num_error_events":       error_count,
    }
    logger.info(f"Stats: {stats}")
    return stats, 200


def health():
    """GET /analyzer/health — never touches Kafka, always instant."""
    return {"status": "healthy"}, 200


# ── App Setup ─────────────────────────────────────────────────────────────────

app = FlaskApp(__name__, specification_dir='')

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api(
    'openapi.yaml',
    base_path='/analyzer',
    strict_validation=True,
    validate_responses=True,
)

if __name__ == '__main__':
    logger.info("Starting Analyzer Service on port 5005")
    app.run(host='0.0.0.0', port=CONFIG['app']['port'])