#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import asyncio
import os

import aiopg

from ipfscontroller import check_files


async def main():
    pool = await aiopg.create_pool(
        f'dbname=docs user=postgres host={os.environ["SERVICEMESH_IP"]}')
    async for cid, _, _ in check_files(pool, "cid", "allocations", "reserves",
                                       "file"):
        print(cid)


if __name__ == "__main__":
    asyncio.run(main())
