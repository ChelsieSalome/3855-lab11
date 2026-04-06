"""
Analyzer Service - app.py
Lab 11 fixes:
  - Uses poll() loop (not iterator) to read Kafka — works reliably with group_id=None.
  - Background thread caches all messages every 2s.
  - API endpoints read from cache instantly.
  - /health never touches Kafka.
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
from kafka import KafkaConsumer, TopicPartition
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

_cache_lock            = threading.Lock()
_cached_messages: list = []


def _read_all_messages() -> list:
    """
    Create a fresh consumer, read ALL messages from the topic from offset 0,
    return them as a list, then close the consumer.
    Uses manual partition assignment (assign + seek) — no group coordination needed.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVER,
        group_id=None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    # Manually assign partition 0 of our topic (single-partition setup)
    tp = TopicPartition(KAFKA_TOPIC, 0)
    consumer.assign([tp])

    # Seek to very beginning
    consumer.seek_to_beginning(tp)

    # Find out how many messages exist
    end_offsets = consumer.end_offsets([tp])
    end_offset  = end_offsets[tp]

    messages = []

    if end_offset == 0:
        consumer.close()
        return messages

    # Poll until we've read everything up to end_offset
    while True:
        records = consumer.poll(timeout_ms=500, max_records=500)
        for _, msgs in records.items():
            for msg in msgs:
                messages.append(msg.value)

        # Check current position
        current_pos = consumer.position(tp)
        if current_pos >= end_offset:
            break

    consumer.close()
    return messages


def _refresh_cache():
    """Background daemon: refreshes message cache every 2 seconds."""
    global _cached_messages
    logger.info("Cache refresh thread started")

    while True:
        try:
            messages = _read_all_messages()
            with _cache_lock:
                _cached_messages = messages
            logger.debug(f"Cache refreshed: {len(messages)} messages")
        except (NoBrokersAvailable, KafkaError) as e:
            logger.warning(f"Kafka unavailable: {e}. Retrying in 5s …")
            time.sleep(5)
            continue
        except Exception as e:
            logger.error(f"Cache refresh error: {e}. Retrying in 5s …")
            time.sleep(5)
            continue

        time.sleep(2)


# Start background cache thread
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
    """GET /analyzer/health — always instant."""
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