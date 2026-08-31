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


async def test_atom_publication_metadata_and_html_cleanup():
    source = RSSSource("https://example.com/feed", source_id="example", source_name="Example")
    source._http = FakeHTTP('''<feed xmlns="http://www.w3.org/2005/Atom">
      <entry><id>article-1</id><title>A &amp; B</title>
      <summary>&lt;p&gt;Clean description&lt;/p&gt;</summary>
      <link rel="self" href="https://example.com/api/1"/>
      <link href="https://example.com/article/1"/>
      <published>2026-09-08T12:00:00Z</published></entry></feed>''')
    messages = await source.poll()
    assert len(messages) == 1
    assert messages[0]["source"] == "rss:example"
    assert messages[0]["source_name"] == "Example"
    assert messages[0]["external_url"] == "https://example.com/article/1"
    assert "<p>" not in messages[0]["text"]
    assert messages[0]["ts"] == 1788868800
    assert await source.poll() == []


async def test_non_feed_html_is_rejected():
    import pytest
    source = RSSSource("https://example.com")
    source._http = FakeHTTP("<html><body>Not a feed</body></html>")
    with pytest.raises(ValueError, match="RSS 2.0 or Atom"):
        await source.poll()
