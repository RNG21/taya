import asyncio
import random
from typing import Any, Dict, Literal, Union, Coroutine, Iterable, List
from asyncio import PriorityQueue, Task

import aiohttp
from aiohttp import ClientSession, ClientResponse


class NoJson(Exception):
    """Raised when response is not json"""

class Response(ClientResponse):
    """for type hinting"""
    text: Union[str, Coroutine]
    json: Any

    def check_nojson(self):
        if self.json is None:
            raise NoJson
        return self.json


async def to_response(response: ClientResponse) -> Response:
    """Make attributes accessible after connection closed"""
    if "json" in response.content_type:
        response.json = await response.json()
    else:
        response.text = await response.text()
        response.json = None
    response.__class__ = Response

    return response  # noqa


class RequestJob:
    def __init__(self, *, priority: int, entry: int, id_: int, kwargs: dict, retries: int = 0):
        """

        Args:
            priority: priority of the job
            entry: entry count of job, to maintain insertion order in heap
            id_: unique id of the job
            retries: how many retries this job is allowed
            kwargs: kwargs for aiohttp.ClientSession.send
        """
        self.priority = priority
        self.entry = entry
        self.id = id_
        self.retries = retries
        self.kwargs = kwargs

    def _compare(self, other, operator: Literal["<", "<=", ">", ">="]):
        if not isinstance(other, RequestJob):
            return NotImplemented

        def cases(attr_name: Literal["priority", "entry"]):
            compare_self = getattr(self, attr_name)
            compare_other = getattr(other, attr_name)
            return {
                "<": compare_self < compare_other,
                "<=": compare_self <= compare_other,
                ">": compare_self > compare_other,
                ">=": compare_self >= compare_other
            }[operator]

        if self.priority == other.priority:
            return cases("entry")
        return cases("priority")

    def __lt__(self, other):
        return self._compare(other, "<")

    def __le__(self, other):
        return self._compare(other, "<=")

    def __gt__(self, other):
        return self._compare(other, ">")

    def __ge__(self, other):
        return self._compare(other, ">=")


class RequestManager:
    """
    Puts all jobs in a PriorityQueue,
    jobs are then removed from the queue and executed,
    results are put into self.results
    """
    queue: "PriorityQueue[RequestJob]"
    results: Dict[int, Response]

    def __init__(self, session: ClientSession, *, rps: int = 3, burst: int = None):
        self.session = session

        self.rps = rps
        if burst is None:
            burst = rps
        self.max = burst
        self.rem = burst  # Amount of request we can send at this very moment

        self.queue = asyncio.PriorityQueue()
        self.results = {}  # {job_id: response}

        self.entry_order = 0
        self.ids = set()
        self.close = False
        self.started = False

    async def start_async_tasks(self):
        assert not self.started
        self.started = True
        asyncio.create_task(self._restore())
        asyncio.create_task(self._execute_jobs())

    async def _restore(self):
        """Restores available requests"""
        while not self.close:
            await asyncio.sleep(1)

            self.rem += self.rps
            if self.rem > self.max:
                self.rem = self.max

    async def _execute_jobs(self):
        """Executes jobs from self.queue and puts results into self.results"""
        async def _do_job(job):
            async with self.session.request(**job.kwargs) as response:
                if response.status != 200 and job.retries > 0:
                    job.retries -= 1
                    await self.queue.put(job)
                else:
                    self.results[job.id] = await to_response(response)

        while not self.close:
            if self.rem > 0 and not self.queue.empty():
                self.rem -= 1
                job = await self.queue.get()
                asyncio.create_task(_do_job(job))
            else:
                await asyncio.sleep(1)

    def _random_id(self) -> int:
        """
        Generates a random ID and puts it into self.ids

        Returns:
            Generated ID
        """
        while True:
            id_ = int(''.join(random.choice("123456789") for _ in range(10)))
            if id_ not in self.ids:
                self.ids.add(id_)
                return id_

    async def retrieve_job(self, job_id: int) -> Response:
        """
        Waits for a job to be executed, retrieves the response and deletes the job
        Args:
            job_id: job id

        Returns:
            response of the job
        """
        # Wait for job to be executed
        while job_id not in self.results:
            await asyncio.sleep(1)
        # Retrieve the response and delete the job
        response = self.results[job_id]
        del self.results[job_id]
        self.ids.remove(job_id)

        return response

    async def _send(self, *, priority: int, retries: int, kwargs: dict) -> Response:
        if not self.started:
            await self.start_async_tasks()

        # Put job into queue
        self.entry_order += 1
        job = RequestJob(
            priority=priority,
            entry=self.entry_order,
            id_=self._random_id(),
            retries=retries,
            kwargs=kwargs
        )
        await self.queue.put(job)

        return await self.retrieve_job(job.id)

    def send(self, *, priority: int = 1, retries: int, kwargs: dict) -> Task[Response]:
        return asyncio.create_task(self._send(priority=priority, retries=retries, kwargs=kwargs))

    def get(self, url: str, *, priority: int = 1, retries: int = 0) -> Task[Response]:
        return asyncio.create_task(
            self._send(priority=priority, retries=retries, kwargs={"method": "GET", "url": url})
        )


if __name__ == "__main__":
    async def main():
        async with aiohttp.ClientSession() as session:
            client = RequestManager(session=session, rps=3, burst=10)
            for i in range(1000):
                client.get(
                    url=f"https://api.warframe.market/v1/items",
                    priority=i,
                    retries=1
                )
            await asyncio.sleep(1)
            while True:
                await asyncio.sleep(60)

    asyncio.run(main())
