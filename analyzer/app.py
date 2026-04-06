"""
Analyzer Service - app.py
Lab 11 fixes:
  - Background thread caches all Kafka messages in memory every 2s.
  - Waits for partition assignment before seek_to_beginning (fixes "No partitions assigned" error).
  - API endpoints read from cache instantly — no per-request Kafka I/O.
  - /health never touches Kafka or the cache.
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

# ── In-memory cache ───────────────────────────────────────────────────────────

_cache_lock              = threading.Lock()
_cached_messages: list   = []


def _make_consumer() -> KafkaConsumer:
    """Connect to Kafka, retrying with exponential backoff."""
    delay = 1
    attempt = 0
    while True:
        attempt += 1
        try:
            c = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                # No group_id so we get a fresh, independent consumer
                # that always reads from the beginning without interfering
                # with the storage consumer group.
                group_id=None,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            )
            logger.info(f"Kafka consumer connected (attempt {attempt})")
            return c
        except (NoBrokersAvailable, KafkaError) as e:
            logger.warning(f"Kafka unavailable (attempt {attempt}): {e}. Retry in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 32)
        except Exception as e:
            logger.error(f"Unexpected Kafka error (attempt {attempt}): {e}. Retry in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 32)


def _refresh_cache():
    """
    Background daemon: reads ALL Kafka messages every 2 s and updates the cache.
    Uses group_id=None so we get our own offset pointer and never need seek_to_beginning
    across a group rebalance — partition assignment is immediate.
    """
    global _cached_messages
    logger.info("Cache refresh thread started")

    while True:
        consumer = _make_consumer()
        try:
            while True:
                # poll() triggers partition assignment; then we can seek safely
                consumer.poll(timeout_ms=500)

                # Seek to beginning of every assigned partition
                partitions = consumer.assignment()
                if not partitions:
                    # Partitions not yet assigned — poll again
                    time.sleep(0.2)
                    continue

                consumer.seek_to_beginning(*partitions)

                # Drain all messages
                messages = []
                for msg in consumer:        # stops when consumer_timeout_ms hit
                    messages.append(msg.value)

                with _cache_lock:
                    _cached_messages = messages

                logger.debug(f"Cache refreshed: {len(messages)} messages")
                time.sleep(2)

        except Exception as e:
            logger.error(f"Cache refresh error: {e}. Reconnecting in 3s …")
            try:
                consumer.close()
            except Exception:
                pass
            time.sleep(3)


# Start background cache thread at import time
_t = threading.Thread(target=_refresh_cache, daemon=True)
_t.start()


def _snapshot() -> list:
    with _cache_lock:
        return list(_cached_messages)


# ── API Endpoints ─────────────────────────────────────────────────────────────

def get_performance_event(index: int):
    """GET /analyzer/performance?index=N"""
    logger.info(f"Request for performance event at index {index}")
    events = [m['payload'] for m in _snapshot() if m.get('type') == 'performance_metric']
    if index >= len(events):
        logger.error(f"No performance event at index {index} (total={len(events)})")
        return {"message": f"No performance event at index {index}"}, 404
    logger.info(f"Returning performance event at index {index}")
    return events[index], 200


def get_error_event(index: int):
    """GET /analyzer/error?index=N"""
    logger.info(f"Request for error event at index {index}")
    events = [m['payload'] for m in _snapshot() if m.get('type') == 'error_metric']
    if index >= len(events):
        logger.error(f"No error event at index {index} (total={len(events)})")
        return {"message": f"No error event at index {index}"}, 404
    logger.info(f"Returning error event at index {index}")
    return events[index], 200


def get_stats():
    """GET /analyzer/stats"""
    logger.info("Request for event statistics")
    msgs        = _snapshot()
    perf_count  = sum(1 for m in msgs if m.get('type') == 'performance_metric')
    error_count = sum(1 for m in msgs if m.get('type') == 'error_metric')
    stats = {"num_performance_events": perf_count, "num_error_events": error_count}
    logger.info(f"Stats: {stats}")
    return stats, 200


def health():
    """GET /analyzer/health — always instant, never touches Kafka."""
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