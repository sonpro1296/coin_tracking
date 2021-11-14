import asyncio
import json
import venv

from source.tracker import PriceTracker
from dispatcher.dispatcher import Dispatcher
from candlestick.generator import CandlestickGenerator
from producer.msgproducer import MsgProducer

import threading

subcribe_msg = json.dumps({
    "type": "subscribe",
    "product_ids": [
        "ETH-USD",
        # "BTC-USD"
    ],
    "channels": [
        # "level2"
        # "heartbeat",
        {
            "name": "ticker",
            "product_ids": [
                "ETH-USD",
                # "BTC-USD"
            ]
        }
    ]
})

tracker = PriceTracker("wss://ws-feed.exchange.coinbase.com", subcribe_msg)
queue = asyncio.Queue()

dispatcher = Dispatcher(tracker.get_queue())
dispatcher.add_output(queue)
generator = CandlestickGenerator(queue)

generator_queue = generator.get_output_queue()
producer = MsgProducer("14.225.254.108:9092", "candlestick", generator_queue)

# tracker.run()
# dispatcher.start()
# generator.run()
event_loop = asyncio.get_event_loop()

# threading.Thread(target=tracker.run, args=(event_loop,)).start()
# threading.Thread(target=dispatcher.start, args=(event_loop,)).start()
# threading.Thread(target=generator.run, args=(event_loop,)).start()

event_loop.create_task(tracker.get_messages())
event_loop.create_task(dispatcher.run())
event_loop.create_task(generator.loop())
event_loop.create_task(producer.run())

event_loop.run_forever()
