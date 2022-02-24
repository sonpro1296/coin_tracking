import asyncio
import json

import socket
from kafka import KafkaProducer


class MsgProducer:
    def __init__(self, bootstrap_server: str, topic: str, queue: asyncio.Queue):
        # conf = {'bootstrap.servers': bootstrap_server}
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server)
        self.topic = topic
        self.input = queue

    async def run(self):
        while True:
            msg = await self.input.get()
            print(msg)
            # print(bytes(msg, encoding='utf-8'))
            self.producer.send(topic=self.topic, value=msg)
            self.input.task_done()
            await asyncio.sleep(0.01)

    def start(self, event_loop: asyncio.events):
        event_loop.run_until_complete(self.run())


