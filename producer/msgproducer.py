import asyncio
import json

import socket
from confluent_kafka import Producer, admin


class MsgProducer:
    def __init__(self, bootstrap_server: str, topic: str, queue: asyncio.Queue):
        conf = {'bootstrap.servers': bootstrap_server}
        self.producer = Producer(conf)
        self.topic = topic
        self.input = queue

    async def run(self):
        while True:
            msg = await self.input.get()
            print(msg)
            # print(bytes(msg, encoding='utf-8'))
            self.producer.produce(topic=self.topic, value=msg, callback=acked)
            self.producer.poll(1)
            self.input.task_done()
            await asyncio.sleep(0.01)

    def start(self, event_loop: asyncio.events):
        event_loop.run_until_complete(self.run())


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))
