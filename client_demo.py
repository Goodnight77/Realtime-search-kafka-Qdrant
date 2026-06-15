import asyncio
import json
import sys

import websockets


async def main() -> None:
    query = " ".join(sys.argv[1:]) or "latest tech news"
    uri = "ws://localhost:8000/ws/search"
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"query": query, "k": 5}))
        async for msg in ws:
            data = json.loads(msg)
            print(f"\n=== hits for '{data['query']}' ===")
            for h in data["hits"]:
                text = (h.get("payload") or {}).get("text", "")
                print(f"  [{h['score']:.3f}] {text}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
