#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import asyncio
import logging
import os

import aiopg
from redis.asyncio import StrictRedis

from ipfsworkerlib import say

logging.basicConfig(level=logging.DEBUG)


async def check_files(pool, cid_column, alloc_column, table):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            cmd = f"""
SELECT {cid_column}, {alloc_column}, replications from {table}
where {cid_column} is not null and state != 'delete' and (
{alloc_column} is null
or
(
            replications is null and jsonb_array_length({alloc_column}) < 2
)
or
(
replications is not null and jsonb_array_length({alloc_column}) < replications
)
)
"""
            await cur.execute(cmd)
            async for row in cur:
                yield row


async def check_files_for_rotation(pool, cid_column, alloc_column,
                                   rotation_column, table):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            cmd = f"""
SELECT {cid_column}, {alloc_column}, {rotation_column} from {table}
where {cid_column} is not null and state != 'delete' and (
{rotation_column} is not null
)
order by jsonb_array_length({alloc_column})
limit 1
"""
            await cur.execute(cmd)
            async for row in cur:
                yield row


async def get_members(connection):
    members = await connection.smembers("ipfsworker.workers")
    return [
        member for member in members
        if await connection.get(f"ipfsworker.{member}.df") is not None
    ]


async def get_candidates(connection, allocations, replications):
    members = await get_members(connection)
    members = set(members) - set(allocations)
    if not members:
        return None
    # exclude node already doing something
    error_members = [
        member for member in members
        if await connection.llen(f"ipfsworker.{member}.error") != 0
    ]
    if error_members:
        say(f"Those node have to recover from errors: {', '.join(error_members)}"
            )
    members = [
        member for member in members
        if await connection.get(f"ipfsworker.{member}.current") is None
        and member not in error_members
    ]
    members_with_df = [(float(await connection.get(f"ipfsworker.{member}.df")),
                        member) for member in members]
    candidates = list(reversed(
        sorted(members_with_df)))[:max(0, replications - len(allocations))]
    return [candidate[1] for candidate in candidates]


async def wait_for_sync(connection):
    client = StrictRedis(host=os.environ["REDIS_HOST"], port=6379, db=0)
    p = client.pubsub()
    await p.subscribe('ipfsworker.controller.wake')
    await p.get_message(timeout=1)  # header
    await p.get_message(timeout=1)  # header
    members = await get_members(connection)
    await connection.publish("ipfsworker.workers.wake", "dummy")
    say("Awakened workers, waiting for idle time")
    seconds = 5
    while not all(await asyncio.gather(
            *[check_member(member, connection) for member in members])):
        say("Workers are still working."
            f" Waiting for {seconds} seconds for someone to wake me")
        message = await p.get_message(timeout=seconds)
        if message is None:
            # nothing unusual, just wait longueur
            seconds = min(seconds * 2, 3600)
        else:
            say("Someone awakened me, let's check if I can stop waiting")
            seconds = 5
    await p.unsubscribe()


async def check_member(member, connection):
    if await connection.get(f"ipfsworker.{member}.df") is None:
        say(f"Heartbeat missed for {member}, removing it from the members")
        await connection.srem("ipfsworker.workers", member)
        return
    res = all([
        await connection.get(f"ipfsworker.{member}.current") is None,
        await connection.llen(f"ipfsworker.{member}") == 0,
        await connection.llen(f"ipfsworker.{member}.error") == 0,
    ])
    return res


async def push_work(connection, pool):
    waiting_time = 60
    await wait_for_sync(connection)
    while True:
        cids = []
        for cid_column, alloc_column, table in [
            ("cid", "allocations", "file"),
            ("thumbnail_cid", "thumbnail_allocations", "photovideo"),
            ("web_cid", "web_allocations", "photovideo"),
            ("sub_cid", "sub_allocations", "film"),
            ("sub_cid", "sub_allocations", "serie"),
        ]:
            say(f"Playing with table {table}")
            async for cid, candidates in step(connection, pool, cid_column,
                                              alloc_column, table):
                say(f"Done with cid {cid}")
                cids.append(cid)
                await wait_for_sync(connection)
                say(f"{' and '.join(candidates)} should now have {cid}")
        if cids == []:
            say("Wake up processes that waited to see whether the normal work ended"
                )
            await connection.publish("ipfsworker.controller.idle", "dummy")
        if cids == []:
            say("Nothing done in the normal case, deal with some rotations")
            for cid_column, alloc_column, rotation_column, table in [
                ("cid", "allocations", "rotation", "file"),
                ("thumbnail_cid", "thumbnail_allocations",
                 "thumbnail_rotation", "photovideo"),
                ("web_cid", "web_allocations", "web_rotation", "photovideo"),
                ("sub_cid", "sub_allocations", "sub_rotation", "film"),
                ("sub_cid", "sub_allocations", "sub_rotation", "serie"),
            ]:
                say(f"Playing with table {table} for rotation")
                async for cid, candidates in rotation_step(
                        connection, pool, cid_column, alloc_column,
                        rotation_column, table):
                    say(f"Done with cid {cid} for rotation")
                    cids.append(cid)
                    await wait_for_sync(connection)
                    say(f"{' and '.join(candidates)} have {cid} for rotation")

        if cids == []:
            say(f"Since nothing was done, waiting {waiting_time}s before starting again"
                )
            subscriber = connection.pubsub()
            await subscriber.subscribe('ipfsworker.controller.wake')
            await subscriber.get_message(timeout=1)  # header
            await subscriber.get_message(timeout=1)  # header
            await subscriber.get_message(timeout=waiting_time)
            await subscriber.unsubscribe("ipfsworker.controller.wake")
            waiting_time = min(waiting_time * 2, 3600)
        else:
            waiting_time = 60


async def step(connection, pool, cid_column, alloc_column, table):
    async for cid, allocations, replications in check_files(
            pool, cid_column, alloc_column, table):
        allocations = allocations or []
        replications = replications or 2
        say(f"Replicating {replications} times {cid},"
            f" already in {', '.join(allocations) if allocations else 'nowhere...'}"
            )
        candidates = await get_candidates(connection, allocations,
                                          replications)
        if candidates is None or candidates == []:
            say(f"No alive node to host {cid}")
            continue
        say(f"Found candidates {', '.join(candidates)}")
        for candidate in candidates:
            say(f"Pushing {cid} to {candidate}")
            await connection.lpush(
                f"ipfsworker.{candidate}",
                cid,
            )
            say(f"Updating alloc for {cid} to {candidate}")
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"""
update {table} set {alloc_column} = (
case
                when {alloc_column} is null then '[]'
                else {alloc_column}
end
) || '["{candidate}"]' where {cid_column} = '{cid}'
""")
            say(f"Done with {cid} to {candidate}")

        yield cid, candidates


async def rotation_step(connection, pool, cid_column, alloc_column,
                        rotation_column, table):
    async for cid, allocations, candidates in check_files_for_rotation(
            pool, cid_column, alloc_column, rotation_column, table):
        allocations = allocations or []
        say(f"Rotating {cid} in {rotation_column} for {candidates}")
        done_for = []
        for candidate in candidates:
            assert candidate in allocations, f"{candidate} is not in allocations ({allocations}): Mistake?"
            isavailable = await check_member(candidate, connection)
            if not isavailable:
                say(f"{candidate} is not available, skipping for now")
                continue
            say(f"Pushing {cid} to {candidate} for rotation")
            await connection.lpush(
                f"ipfsworker.{candidate}",
                cid,
            )
            say(f"Consuming the rotation for candidate {candidate} for {cid}")
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"""
update {table} set {rotation_column} = (
case
    when {rotation_column} = '["{candidate}"]'
    then null
    else {rotation_column} - '{candidate}'
    end
)
where {cid_column} = '{cid}'
""")
            say(f"Updated the database for {cid} to {candidate} for rotation")
            done_for.append(candidate)
        if done_for:
            yield cid, candidates


async def main():
    connection = StrictRedis(
        decode_responses=True,
        host=os.environ["REDIS_HOST"],
    )
    say("Connecting to postgres")
    pool = await aiopg.create_pool(
        f'dbname=docs user=postgres host={os.environ["SERVICEMESH_IP"]}')
    say("Connected to postgres")

    await asyncio.gather(push_work(connection, pool))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
