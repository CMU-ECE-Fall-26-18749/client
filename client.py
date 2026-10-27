"""Milestone 3 client: connect to all replicas and send only to S1."""

import argparse
import asyncio
import contextlib
import json

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from utils import log


class PassiveClient:
    def __init__(self, args):
        self.args = args
        self.epoch = None
        self.version = -1
        self.primary_id = None
        self.members = {}
        self.connections = {}
        self.orders = {}
        self.requests = []
        self.replies = {}
        self.control_ws = None
        self.changed = asyncio.Event()

    async def wait_for(self, predicate):
        while not predicate():
            self.changed.clear()
            if not predicate():
                await self.changed.wait()

    async def update_membership(self, message):
        if self.epoch and message["epoch"] != self.epoch:
            raise RuntimeError("GFD restarted; restart the complete demo")
        self.epoch = message["epoch"]
        if message["version"] <= self.version:
            return
        self.version = message["version"]
        self.primary_id = message["primary_id"]
        new_members = {m["replica_id"]: m for m in message["members"]}

        for replica_id in list(self.connections):
            if replica_id not in new_members:
                task = self.connections.pop(replica_id)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        self.members = new_members
        for replica_id, member in new_members.items():
            if replica_id not in self.connections:
                self.connections[replica_id] = asyncio.create_task(
                    self.replica_connection(member)
                )

        log(
            f"{self.args.client_id}: Membership v{self.version}: "
            f"{', '.join(sorted(new_members)) or '(empty)'}; "
            f"primary={self.primary_id}",
            "membership",
        )
        self.changed.set()

    async def gfd_connection(self):
        url = self.args.gfd_url.rstrip("/") + f"/ws/client/{self.args.client_id}"
        while True:
            try:
                async with connect(url, proxy=None, open_timeout=3,
                                   close_timeout=1) as ws:
                    self.control_ws = ws
                    self.changed.set()
                    async for raw in ws:
                        message = json.loads(raw)
                        if message["type"] == "membership":
                            await self.update_membership(message)
                        elif message["type"] == "ordered":
                            self.orders[message["request_num"]] = message
                            self.changed.set()
                        elif message["type"] == "unavailable":
                            log(f"{self.args.client_id}: S1 unavailable; waiting",
                                "fault")
            except (OSError, TimeoutError, WebSocketException):
                log(f"{self.args.client_id}: GFD connection lost", "fault")
            finally:
                self.control_ws = None
                self.changed.set()
            await asyncio.sleep(0.5)

    async def replica_connection(self, member):
        replica_id = member["replica_id"]
        url = member["url"].rstrip("/") + f"/ws/client/{self.args.client_id}"
        acknowledged = 0
        while True:
            tasks = []
            try:
                async with connect(url, proxy=None, open_timeout=2,
                                   close_timeout=1) as ws:
                    log(f"{self.args.client_id}: Connected to {replica_id}", "info")
                    if replica_id != "S1":
                        await asyncio.Future()

                    async def send_requests():
                        index = acknowledged
                        while True:
                            await self.wait_for(lambda: index < len(self.requests))
                            request = self.requests[index]
                            await ws.send(json.dumps(request))
                            log(
                                f"{self.args.client_id}: Sent "
                                f"<{self.args.client_id}, S1, "
                                f"{request['request_num']}, request>",
                                "request",
                            )
                            index += 1

                    async def receive_replies():
                        nonlocal acknowledged
                        while True:
                            raw = await asyncio.wait_for(ws.recv(), self.args.timeout)
                            reply = json.loads(raw)
                            request_num = reply["request_num"]
                            request = self.requests[request_num - 1]
                            if (reply.get("type") != "reply" or
                                    reply.get("replica_id") != "S1" or
                                    reply.get("global_seq") != request["global_seq"]):
                                raise RuntimeError("invalid reply from S1")
                            if request_num not in self.replies:
                                self.replies[request_num] = reply
                                log(
                                    f"{self.args.client_id}: Received "
                                    f"<{self.args.client_id}, S1, "
                                    f"{request_num}, reply> "
                                    f"server_state={reply['my_state']}",
                                    "reply",
                                )
                                log(f"{self.args.client_id}: request_num "
                                    f"{request_num}: Delivered reply", "reply")
                            acknowledged = max(acknowledged, request_num)
                            self.changed.set()

                    tasks = [
                        asyncio.create_task(send_requests()),
                        asyncio.create_task(receive_replies()),
                    ]
                    done, _ = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in done:
                        task.result()
            except (OSError, TimeoutError, WebSocketException):
                log(f"{self.args.client_id}: {replica_id} connection lost",
                    "fault")
                await asyncio.sleep(0.3)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def produce_requests(self):
        request_num = 1
        while not self.args.max_requests or request_num <= self.args.max_requests:
            await self.wait_for(
                lambda: self.control_ws is not None and "S1" in self.members
            )
            while request_num not in self.orders:
                try:
                    await self.control_ws.send(json.dumps({
                        "type": "order",
                        "request_num": request_num,
                        "payload": f"request_{request_num}_from_{self.args.client_id}",
                    }))
                    await asyncio.wait_for(
                        self.wait_for(lambda: request_num in self.orders),
                        self.args.timeout,
                    )
                except (AttributeError, OSError, TimeoutError, WebSocketException):
                    await asyncio.sleep(0.3)
            self.requests.append(self.orders[request_num])
            self.changed.set()
            await self.wait_for(lambda: request_num in self.replies)
            request_num += 1
            await asyncio.sleep(self.args.interval)

    async def run(self):
        tasks = [
            asyncio.create_task(self.gfd_connection()),
            asyncio.create_task(self.produce_requests()),
        ]
        try:
            await tasks[1]
        finally:
            for task in [*tasks, *self.connections.values()]:
                task.cancel()
            await asyncio.gather(
                *tasks,
                *self.connections.values(),
                return_exceptions=True,
            )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client_id", required=True)
    parser.add_argument("--gfd_url", default="ws://127.0.0.1:9000")
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--max_requests", type=int, default=0)
    args = parser.parse_args()
    if args.interval < 0 or args.timeout <= 0 or args.max_requests < 0:
        parser.error("invalid timing or request-count value")
    return args


if __name__ == "__main__":
    try:
        asyncio.run(PassiveClient(parse_args()).run())
    except KeyboardInterrupt:
        pass
