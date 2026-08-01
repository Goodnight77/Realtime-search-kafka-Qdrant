from prometheus_client import Counter, Gauge, Histogram

INGEST_TOTAL = Counter(
    "ingest_total", "Messages upserted into Qdrant", ["source"]
)
SEARCH_LATENCY = Histogram(
    "search_latency_seconds", "search() call latency"
)
KAFKA_CONSUMER_LAG = Gauge(
    "kafka_consumer_lag", "highwater - offset for the last consumed message", ["topic", "partition"]
)
