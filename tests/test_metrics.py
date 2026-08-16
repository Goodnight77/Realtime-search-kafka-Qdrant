from app.api import metrics
from app.metrics import INGEST_TOTAL


async def test_metrics_endpoint_exposes_prometheus_format():
    INGEST_TOTAL.labels(source="hn:story").inc()

    resp = await metrics()
    body = resp.body.decode()

    assert "ingest_total" in body
    assert "search_latency_seconds" in body
    assert "kafka_consumer_lag" in body
