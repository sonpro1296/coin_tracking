import asyncio
import json
import tenacity
from websockets import connect
import multiprocessing


class PriceTracker:
    def __init__(self, uri: str, msg: json):
        self.uri = uri
        self.msg = msg
        self.queue = asyncio.Queue()
        pass

    def run(self, event_loop: asyncio.events):
        event_loop.run_until_complete(self.get_messages())

    @tenacity.retry(wait=tenacity.wait_fixed(1))
    async def get_messages(self):
        async with connect(self.uri, ping_interval=None) as ws:
            await ws.send(self.msg)
            while True:
                try:
                    d = await ws.recv()
                    await self.queue.put(d)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print("print error: ", e)


    def get_queue(self):
        return self.queue


sub_msg = json.dumps({
    "type": "subscribe",
    "product_ids": [
        "ETH-USD",
        "BTC-USD"
    ],
    "channels": [
        # "level2"
        # "heartbeat",
        {
            "name": "ticker",
            "product_ids": [
                "ETH-USD",
                "BTC-USD"
            ]
        }
    ]
})
