import asyncio
import json


class Dispatcher:
    def __init__(self, queue: asyncio.Queue):
        self.input = queue
        self.list_output = list()

    def add_output(self, queue: asyncio.Queue):
        self.list_output.append(queue)

    def dispatch(self, msg: json):
        for q in self.list_output:
            q.put_nowait(msg)

    async def run(self):
        while True:
            msg = await self.input.get()
            # print("dispatcher: ", msg)
            self.dispatch(msg)
            self.input.task_done()
            await asyncio.sleep(0.01)

    def start(self, event_loop: asyncio.events):
        event_loop.run_until_complete(self.run())
