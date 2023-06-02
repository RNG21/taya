import asyncio
import random
from typing import Any

import aiohttp
from aiohttp import ClientSession, ClientResponse


class Response(ClientResponse):
    """for type hinting"""
    text: str
    content: str
    json: Any


async def to_response(response: ClientResponse) -> Response:
    """Make attributes accessible after connection closed"""
    response.text = await response.text()
    response.content = await response.content.read()
    if "json" in response.content_type:
        response.json = await response.json()
    else:
        response.json = None

    return response  # noqa


class RequestJob:
    def __init__(self, *, priority: int, id_: int, kwargs: dict):
        self.priority = priority
        self.id = id_
        self.kwargs = kwargs
        self.response = None

    def __lt__(self, other):
        return self.priority < other.priority
    def __le__(self, other):
        return self.priority <= other.priority
    def __gt__(self, other):
        return self.priority > other.priority
    def __ge__(self, other):
        return self.priority >= other.priority


class RequestManager:
    def __init__(self, session: ClientSession, *, rps: int = 3, burst: int = None):
        self.session = session

        self.rps = rps
        if burst is None:
            burst = rps
        self.max = burst
        self.rem = burst  # Amount of request we can send at this very moment

        self.queue = asyncio.PriorityQueue()
        self.results = {}  # {job_id: response}

        self.ids = set()
        self.close = False

        self.started = False

    async def start_async_tasks(self):
        assert not self.started
        asyncio.create_task(self._restore())
        asyncio.create_task(self._execute_jobs())
        self.started = True

    async def _restore(self):
        """Restores available requests"""
        while not self.close:
            await asyncio.sleep(1)

            self.rem += self.rps
            if self.rem > self.max:
                self.rem = self.max

    async def _execute_jobs(self):
        async def _do_job():
            job = await self.queue.get()
            async with self.session.request(**job.kwargs) as response:
                self.results[job.id] = await to_response(response)

        while not self.close:
            if self.rem > 0 and not self.queue.empty():
                self.rem -= 1
                asyncio.create_task(_do_job())
            else:
                await asyncio.sleep(1)

    def _random_id(self):
        while True:
            id_ = int(''.join(random.choice("123456789") for _ in range(10)))
            if id_ not in self.ids:
                self.ids.add(id_)
                return id_

    async def send(self, *, priority: int = 1, **kwargs) -> Response:
        if not self.started:
            await self.start_async_tasks()

        job = RequestJob(
            priority=priority,
            id_=self._random_id(),
            kwargs=kwargs
        )
        await self.queue.put(job)

        while job.id not in self.results:
            await asyncio.sleep(1)
        response = self.results[job.id]
        del self.results[job.id]
        self.ids.remove(job.id)

        return response

    async def get(self, *, url: str, priority: int = 1) -> Response:
        return await self.send(priority=priority, method="get", url=url)


if __name__ == "__main__":
    async def main():
        async with aiohttp.ClientSession() as session:
            client = RequestManager(session=session, rps=3, burst=10)
            for i in range(20):
                for _ in range(2):
                    asyncio.create_task(client.get(priority=i, url="https://api.warframe.market/v1/items"))
            while True:
                await asyncio.sleep(30)


    asyncio.run(main())
