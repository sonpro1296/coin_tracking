import asyncio
import json

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

    async def get_messages(self):
        async with connect(self.uri, ping_interval=None) as ws:
            await ws.send(self.msg)
            while True:
                d = await ws.recv()
                # print("tracker: ", d)
                await self.queue.put(d)
                # print("putted to queue")
                await asyncio.sleep(0.1)

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



