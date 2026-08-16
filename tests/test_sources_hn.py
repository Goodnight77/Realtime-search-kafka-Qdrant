import json

from app.sources.hn import HNSource, _to_msg


def test_to_msg_normalizes_story():
    item = {
        "type": "story", "title": "Hello", "text": None,
        "id": 1, "time": 100, "by": "alice", "url": "http://x",
    }
    msg = _to_msg(item)
    assert msg["source"] == "hn:story"
    assert msg["text"] == "Hello"
    assert msg["id"] == 1
    assert msg["author"] == "alice"


def test_to_msg_drops_dead_deleted_placeholder_and_unknown_type():
    assert _to_msg(None) is None
    assert _to_msg({"type": "comment", "dead": True, "text": "hi"}) is None
    assert _to_msg({"type": "comment", "deleted": True, "text": "hi"}) is None
    assert _to_msg({"type": "comment", "text": "[dead]"}) is None
    assert _to_msg({"type": "unknown", "text": "hi"}) is None


class FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        pass

    def json(self):
        return json.loads(self.text)


class FakeHTTP:
    def __init__(self, responses):
        self._responses = responses

    async def get(self, url):
        return self._responses[url]


async def test_hn_source_poll_fetches_new_items_since_last_id():
    source = HNSource()
    source._last_id = 100
    source._http = FakeHTTP(
        {
            "https://hacker-news.firebaseio.com/v0/maxitem.json": FakeResponse("102"),
            "https://hacker-news.firebaseio.com/v0/item/101.json": FakeResponse(
                '{"id": 101, "type": "comment", "text": "hello world", "time": 1000}'
            ),
            "https://hacker-news.firebaseio.com/v0/item/102.json": FakeResponse("null"),
        }
    )

    messages = await source.poll()

    assert len(messages) == 1
    assert messages[0]["id"] == 101
    assert source._last_id == 102


async def test_hn_source_poll_returns_empty_when_no_new_items():
    source = HNSource()
    source._last_id = 100
    source._http = FakeHTTP({"https://hacker-news.firebaseio.com/v0/maxitem.json": FakeResponse("100")})

    messages = await source.poll()

    assert messages == []
