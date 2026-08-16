from app.sources.rss import RSSSource

FEED_XML = """<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title>First post</title>
      <description>Some description</description>
      <link>http://example.com/1</link>
      <guid>guid-1</guid>
      <pubDate>Mon, 01 Aug 2026 12:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Second post</title>
      <description>Another one</description>
      <link>http://example.com/2</link>
      <guid>guid-2</guid>
      <pubDate>Mon, 01 Aug 2026 13:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>"""


class FakeResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


class FakeHTTP:
    def __init__(self, text):
        self._text = text

    async def get(self, url):
        return FakeResponse(self._text)


async def test_rss_source_parses_items_and_dedupes_by_guid():
    source = RSSSource(feed_url="http://example.com/feed.xml")
    source._http = FakeHTTP(FEED_XML)

    first = await source.poll()
    assert len(first) == 2
    assert {m["id"] for m in first} == {"guid-1", "guid-2"}
    assert all(m["source"] == "rss:item" for m in first)
    assert first[0]["ts"] > 0

    second = await source.poll()
    assert second == []  # same feed content, both guids already seen


async def test_rss_source_requires_feed_url():
    source = RSSSource(feed_url="")
    try:
        await source.start()
        assert False, "expected ValueError"
    except ValueError:
        pass
