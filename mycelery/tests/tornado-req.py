#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: gz


import aiohttp
import time
import asyncio


async def get(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.text()

total_t = 0

url='http://127.0.0.1:8905/'

async def work(i):
    for item in range(10):
        t1 = time.time()
        res = await get(url)
        t2 =time.time()
        global total_t
        total_t += t2-t1
        print(i,t2-t1,res)


loop = asyncio.get_event_loop()
tasks = [work(i) for i in range(300)]
loop.run_until_complete(asyncio.gather(*tasks))

print(total_t)