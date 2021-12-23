import asyncio
import json
import datetime


class CandleStick:
    def __init__(self, low, high, o, c, volume):
        self.ts = None
        self.low = low
        self.high = high
        self.open = o
        self.close = c
        self.volume = volume

    def __repr__(self):
        return f'open: {self.open}, close: {self.close}, high: {self.high}, low:{self.low}, volume: {self.volume}'

    def toJSONWithMinuteMark(self, dt: datetime.datetime):
        self.ts = int(dt.replace(second=0, microsecond=0).timestamp())
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True)


class CandlestickGenerator:
    def __init__(self, queue: asyncio.Queue):
        self.input_queue = queue
        self.output_queue = asyncio.Queue()
        self.first_dt = None
        self.last_dt = None
        self.current_min = 0
        self.candlestick = None
        self.seq = 0

    async def loop(self):
        while True:
            msg = await self.input_queue.get()
            json_msg = json.loads(msg)
            if json_msg["type"] != "ticker":
                continue
            print("candlestick: ", json_msg)
            seq, price, dt, volume = self.get_data_from_msg(json_msg)
            if seq < self.seq:
                return
            self.seq = seq
            minute = dt.minute
            if self.candlestick is None:
                self.current_min = minute
                self.candlestick = CandleStick(price, price, price, price, volume)
                self.last_dt = dt
                self.first_dt = dt
            elif minute == self.current_min:
                new_low = min(self.candlestick.low, price)
                new_high = max(self.candlestick.high, price)
                new_open = self.candlestick.open
                if dt < self.first_dt:
                    new_open = price
                    self.first_dt = dt
                new_close = self.candlestick.close
                if dt > self.last_dt:
                    new_close = price
                    self.last_dt = dt
                new_volume = self.candlestick.volume + volume
                self.candlestick = CandleStick(new_low, new_high, new_open, new_close, new_volume)
            else:
                await self.output_queue.put(self.candlestick.toJSONWithMinuteMark(dt))
                self.candlestick = CandleStick(price, price, price, price, volume)
                self.last_dt = dt
                self.first_dt = dt
                self.current_min = minute

            await asyncio.sleep(0.01)
            self.input_queue.task_done()

    def run(self, event_loop: asyncio.events):
        event_loop.run_until_complete(self.loop())

    @staticmethod
    def get_data_from_msg(msg: json):
        price = float(msg["price"])
        time = msg["time"]
        volume = float(msg["last_size"])
        seq = msg["sequence"]
        dt = datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ')
        return seq, price, dt, volume

    def get_output_queue(self) -> asyncio.Queue:
        return self.output_queue
