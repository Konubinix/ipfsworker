#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import asyncio
import json
import logging
import os
import shutil
import subprocess
import tempfile

from redis.asyncio import StrictRedis

from ipfsworkerlib import say

logging.basicConfig(level=logging.DEBUG)


async def add_files(client, hostname):
    p = client.pubsub()
    await p.subscribe('ipfsworker.workers.wake')

    tmp_file = tempfile.NamedTemporaryFile(delete=True)
    tmp_file.close()
    waiting_time = 2
    while True:
        res = await step(client, hostname, tmp_file)
        if res is None:
            say(f"Did nothing with ipfs, waiting {waiting_time}s")
            message = await p.get_message(timeout=waiting_time)
            if message is None:
                waiting_time = min(waiting_time * 2, 3600)
            else:
                waiting_time = 2
        else:
            waiting_time = 2


async def step(client, hostname, tmp_file):
    cid = await client.get(f"ipfsworker.{hostname}.current", )
    if cid is None:
        if await client.llen(f"ipfsworker.{hostname}") > 0:
            say("Getting from the work list")
            _, value = await client.blpop(f"ipfsworker.{hostname}")
            await client.set(f"ipfsworker.{hostname}.current", value)
            cid = value
        elif await client.llen(f"ipfsworker.{hostname}.error") > 0:
            say("Getting from the error list to try again")
            _, value = await client.blpop(f"ipfsworker.{hostname}.error")
            await client.set(f"ipfsworker.{hostname}.current", value)
            cid = value
        else:
            return None
    else:
        say(f"Continuing with {cid}")
    say(f"Getting {cid}")
    cmd = f"""curl --no-buffer --fail --silent --show-error --location -X POST "http://{os.environ["IPFS_HOST"]}:5001/api/v0/pin/add?arg={cid}&progress=true" """
    say(cmd)
    proc = await asyncio.create_subprocess_shell(cmd)
    await proc.communicate()
    ok = proc.returncode == 0
    say("Now, checking that the content is actually pinned")
    cmd = f"""curl -X POST "http://{os.environ["IPFS_HOST"]}:5001/api/v0/pin/ls?arg={cid}" """
    say(cmd)
    lsproc = await asyncio.create_subprocess_shell(cmd, stdout=subprocess.PIPE)
    out, err = await lsproc.communicate()
    if cid[len("/ipfs/"):] not in json.loads(out)["Keys"]:
        say("Could not find {cid} in the pins => error")
        ok = False
    res = None
    if ok:
        say("Everything looks fine. Dropping the pin now and moving on")
        cmd = f"""curl -X POST "http://{os.environ["IPFS_HOST"]}:5001/api/v0/pin/rm?arg={cid}" """
        say(cmd)
        rmproc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=subprocess.PIPE,
        )
        out, err = await rmproc.communicate()
        if json.loads(out) != {"Pins": [cid[len("/ipfs/"):]]}:
            say("Something went wrong when dropping the pin. Just to be safe, let's not consider this cid got"
                )
            say(out)
            ok = False
    if ok:
        await client.lpush(
            f"ipfsworker.{hostname}.done",
            cid,
        )
        say(f"Got {cid}")
        res = cid
    else:
        await client.lpush(
            f"ipfsworker.{hostname}.error",
            cid,
        )
        say("Something went wrong")
    await client.delete(f"ipfsworker.{hostname}.current", )
    await client.publish("ipfsworker.controller.wake", "dummy")
    return res


async def inform_remaining_size(client, hostname):
    while True:
        usage = shutil.disk_usage(
            os.path.expanduser("~/.ipfs")).free / (1024 * 1024 * 1024)
        await client.set(
            f"ipfsworker.{hostname}.df",
            str(usage),
            ex=120,
        )
        await asyncio.sleep(60)


async def inform_alive(client, hostname):
    while True:
        await client.sadd('ipfsworker.workers', hostname)
        await asyncio.sleep(60)


async def main():
    client = StrictRedis(
        decode_responses=True,
        host=os.environ["REDIS_HOST"],
    )
    hostname = os.environ["WORKER_NAME"]
    # simply tell the controller that it might be awaken to send some work
    await client.publish("ipfsworker.controller.wake", "dummy")
    await asyncio.gather(
        add_files(client, hostname),
        inform_remaining_size(client, hostname),
        inform_alive(client, hostname),
    )

    client.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
