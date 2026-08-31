from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from qdrant_client.http.exceptions import ResponseHandlingException

from app import api


async def test_health_recovers_from_transient_dns_failure(monkeypatch):
    client = AsyncMock()
    client.count.side_effect = [ResponseHandlingException(OSError("DNS failed")), SimpleNamespace(count=42)]
    monkeypatch.setattr(api, "_ready", True)
    monkeypatch.setattr(api, "get_client", lambda: client)
    result = await api.health()
    assert result["status"] == "ok"
    assert result["in_window"] == 42
    assert client.count.await_count == 2


async def test_health_reports_persistent_failure_as_503(monkeypatch):
    client = AsyncMock()
    client.count.side_effect = ResponseHandlingException(OSError("DNS failed"))
    monkeypatch.setattr(api, "_ready", True)
    monkeypatch.setattr(api, "get_client", lambda: client)
    with pytest.raises(HTTPException) as error:
        await api.health()
    assert error.value.status_code == 503
    assert client.count.await_count == 2
