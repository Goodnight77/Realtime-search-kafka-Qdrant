from types import SimpleNamespace
from unittest.mock import AsyncMock

from app import news


async def test_recent_articles_filter_sort_dedup_and_safe_links(monkeypatch):
    client = AsyncMock()
    def point(i, **values):
        return SimpleNamespace(id=i, payload={"source": "hn:story", "ts": 999, "title": "A headline", **values})
    client.scroll.return_value = ([
        point("bad", external_url="javascript:alert(1)"),
        point("1", external_url="https://example.com/story", text="Long article body"),
        point("2", external_url="https://example.com/story#comments"),
        point("3", hn_url="https://news.ycombinator.com/item?id=3", title=None, text="Older article"),
    ], None)
    monkeypatch.setattr(news, "get_client", lambda: client)
    monkeypatch.setattr(news.time, "time", lambda: 100000)
    articles = await news.latest_news()
    assert [article["id"] for article in articles] == ["1", "3"]
    assert articles[0]["title"] == "A headline"
    assert articles[1]["title"] == "Older article"
    assert articles[0]["source"] == "Hacker News"
    args = client.scroll.call_args.kwargs
    assert args["order_by"].direction == "desc"
    assert args["with_vectors"] is False
    conditions = args["scroll_filter"].must
    assert conditions[0].range.gte == 100000 - min(news.settings.window_seconds, 86400)
    assert conditions[0].range.lte == 100000
    assert "hn:comment" not in conditions[1].match.any
    assert "hn:story" in conditions[1].match.any


async def test_sidebar_is_limited_to_five_articles(monkeypatch):
    client = AsyncMock()
    client.scroll.return_value = ([SimpleNamespace(id=str(i), payload={
        "title": f"Article {i}", "external_url": f"https://example.com/{i}",
        "ts": 1000, "source": "rss:lobsters", "source_name": "Lobsters",
    }) for i in range(10)], None)
    monkeypatch.setattr(news, "get_client", lambda: client)
    articles = await news.latest_news()
    assert len(articles) == 5
    assert articles[0]["source"] == "Lobsters"
