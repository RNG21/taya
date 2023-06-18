from asyncio import Task
from typing import List, Tuple, AsyncGenerator

import aiohttp
import asqlite
import discord
from aiohttp import ClientSession
from asqlite import Connection
from discord.ext import commands

from core.request_manager import RequestManager, Response


class Wfm:
    def __init__(self, session: ClientSession, conn: Connection):
        self.client = RequestManager(session, rps=3, burst=10)
        self.base = "https://api.warframe.market/v1"
        self.db = conn

    async def get_items(self, priority: int = 1, retries=1) -> list:
        response = await self.client.get(f"{self.base}/items", priority=priority, retries=retries)
        return response.check_nojson()["payload"]["items"]

    async def get_orders(self, url_name: str, priority: int = 1, retries: int = 1) -> list:
        response = await self.client.get(f"{self.base}/items/{url_name}/orders", priority=priority, retries=retries)
        return response.check_nojson()["payload"]["orders"]

    async def get_item_info(self, url_name: str, priority: int = 1, retries: int = 1) -> list:
        response = await self.client.get(f"{self.base}/items/{url_name}", priority=priority, retries=retries)
        return response.check_nojson()["payload"]["item"]["items_in_set"]

    async def get_ducat_info(self, priority: int = 1, retries: int = 1) -> list:
        response = await self.client.get(f"{self.base}/tools/ducats", priority=priority, retries=retries)
        return response.check_nojson()["payload"]["previous_day"]

    async def create_ducats(self) -> None:
        """Creates a table for ducat info in db"""
        output = []
        lookup = {}

        # Populate id, ducat value and median price
        for i, ducat in enumerate(await self.get_ducat_info()):
            lookup[ducat["item"]] = i
            output.append([
                ducat["item"],
                "",
                "",
                ducat["ducats"],
                ducat["median"]
            ])
        # Populate url_name and item_name
        for item in await self.get_items():
            if item["id"] in lookup:
                i = lookup[item["id"]]
                output[i][1] = item["url_name"]
                output[i][2] = item["item_name"]

        await self.db.execute("""
            CREATE TABLE ducat_items(
                item_id    CHAR(24) NOT NULL,
                url_name   VARCHAR(50),
                item_name  VARCHAR(50),
                ducats     SMALLINT,
                median     SMALLINT,
                PRIMARY KEY (item_id)
            );""")
        await self.db.executemany("INSERT INTO ducat_items VALUES (?, ?, ?, ?, ?);", output)

    async def update_ducats(self) -> None:
        """Updates the ducat table in db"""
        await self.db.executemany(
            "UPDATE ducat_items "
            "SET ducats = :ducats, median = :median "
            "WHERE item_id = :item;",
            await self.get_ducat_info()
        )

    async def get_ducats_orders(
            self,
            ducat: int = 100,
            max_median: int = 20,
    ) -> List[Tuple[str, Task[Response]]]:
        """Bunch sends get requests for live orders

        Args:
            ducat: items with this ducat value will be queried
            max_median: get orders whose median is under or equal to this value

        Returns:
            List with schema [(item_name, Task[Response]), ...]
        """
        cursor = await self.db.execute(f"SELECT * FROM ducat_items WHERE ducats={ducat} AND median<={max_median}")
        return [
            (row[2], self.client.get(f"{self.base}/items/{row[1]}/orders", priority=1, retries=1))
            for row in await cursor.fetchall()
        ]

    @staticmethod
    async def parse_ducat_orders(
            tasks: List[Tuple[str, Task[Response]]],
            max_price: int = 12,
            min_quantity: int = 4
    ) -> AsyncGenerator[str, None]:
        """Filter and parse ducat orders

        Args:
            tasks: [(item_name, Task[Response]), ...]
            max_price: upper price limit for an order to be included
            min_quantity: minimum item quantity limit for a seller to be included

        Returns:
            AsyncGenerator with ingame DM strings
        """
        # {seller_ign: [(item_name, price, quantity), ...]}
        sellers = {}
        for item_name, task in tasks:
            orders = (await task).check_nojson()["payload"]["orders"]
            for order in orders:
                order_type, price, quantity = order["order_type"], order["platinum"], order["quantity"]
                ign, status = order["user"]["ingame_name"], order["user"]["status"]

                if order_type == "sell" and price <= max_price and status == "ingame":
                    if ign not in sellers:
                        sellers[ign] = {
                            "string": f"/w {ign} Hi I want to buy",
                            "quantity": 0
                        }
                    sellers[ign]["quantity"] += quantity
                    sellers[ign]["string"] += f' "{item_name}" x{quantity} for {price} platinum,'
                    if quantity > 1:
                        sellers[ign]["string"] = sellers[ign]["string"][:-1] + " each,"

        for ign, value in sellers.items():
            if value["quantity"] >= min_quantity:
                yield value["string"][:-1] + " (warframe.market)"


if __name__ == "__main__":
    import asyncio

    async def main():
        async with aiohttp.ClientSession() as session, asqlite.connect("../wfm.db") as conn:
            wfm = Wfm(session, conn)
            print("doing")
            async for string in wfm.parse_ducat_orders(await wfm.get_ducats_orders(), min_quantity=4):
                print(string)

            print("completed")
            await conn.commit()
            while True:
                await asyncio.sleep(60)

    asyncio.run(main())
