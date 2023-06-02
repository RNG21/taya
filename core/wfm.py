import aiohttp
from aiohttp import ClientSession

from request_manager import RequestManager


class WfmClient:
    def __init__(self, session: ClientSession):
        self.client = RequestManager(session, rps=3, burst=10)
        self.items = []
        self.base = "https://api.warframe.market/v1"

    async def get_items(self):
        response = await self.client.get(url=f"{self.base}/items")
        self.items = response.json["payload"]
        return self.items

    async def get_orders(self, url_name):
        return await self.client.get(url=f"{self.base}/items/{url_name}/orders")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with aiohttp.ClientSession() as session:
            client_ = WfmClient(session)
            print(await client_.get_items())

    asyncio.run(main())
