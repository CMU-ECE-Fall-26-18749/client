"""Send requests to active replicas or the current passive primary."""

import argparse
import asyncio
import json

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from utils import log


class ReplicatedClient:
    def __init__(self, args):
        self.args = args
        self.epoch = None
        self.version = -1
        self.primary_id = None
        self.mode = None
        self.members = {}
        self.connections = {}
        self.sent = {}
        self.orders = {}
        self.requests = []
        self.replies = {}
        self.control_ws = None
        self.changed = asyncio.Event()
        self.failure = None

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
        self.primary_id = message.get("primary_id")
        self.mode = message.get("mode", "passive")
        new_members = {m["replica_id"]: m for m in message["members"]}

        previous = self.members
        self.members = new_members
        for replica_id in list(self.connections):
            member = new_members.get(replica_id)
            if not member or any(previous[replica_id].get(key) != member.get(key)
                                 for key in ("url", "boot_id")):
                task = self.connections.pop(replica_id)
                self.sent.pop(replica_id, None)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        for replica_id, member in new_members.items():
            if replica_id not in self.connections:
                task = asyncio.create_task(self.replica_connection(member))
                task.add_done_callback(self.connection_done)
                self.connections[replica_id] = task

        log(
            f"{self.args.client_id}: Membership v{self.version}: "
            f"{', '.join(sorted(new_members)) or '(empty)'}; "
            f"mode={self.mode}; primary={self.primary_id}",
            "membership",
        )
        self.changed.set()

    def connection_done(self, task):
        if not task.cancelled() and task.exception() and not self.failure.done():
            self.failure.set_exception(task.exception())

    def ready_members(self):
        return [replica_id for replica_id, member in self.members.items()
                if member.get("ready", True)]

    def can_reply(self, replica_id):
        return (replica_id in self.members
                and self.members[replica_id].get("ready", True)
                and (self.mode == "active" or replica_id == self.primary_id))

    def can_send(self, replica_id):
        return replica_id in self.members and (
            self.mode == "active" or self.can_reply(replica_id)
        )

    def available(self):
        return bool(self.ready_members()) if self.mode == "active" else self.can_reply(
            self.primary_id
        )

    async def gfd_connection(self):
        url = self.args.gfd_url.rstrip("/") + f"/ws/client/{self.args.client_id}"
        while True:
            try:
                async with connect(url, proxy=None, open_timeout=3,
                                   close_timeout=1) as ws:
                    async for raw in ws:
                        message = json.loads(raw)
                        if message["type"] == "membership":
                            await self.update_membership(message)
                            self.control_ws = ws
                            self.changed.set()
                        elif message["type"] == "ordered":
                            self.orders[message["request_num"]] = message
                            self.changed.set()
                        elif message["type"] == "unavailable":
                            log(f"{self.args.client_id}: No serving replica; waiting",
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
        while True:
            tasks = []
            try:
                async with connect(url, proxy=None, open_timeout=2,
                                   close_timeout=1) as ws:
                    log(f"{self.args.client_id}: Connected to {replica_id}", "info")
                    pending = {}
                    sent = len(self.replies)
                    if self.mode == "active":
                        sent = min(sent, max(0, len(self.requests) - 1))

                    async def send_requests():
                        nonlocal sent
                        while True:
                            await self.wait_for(lambda: self.can_send(replica_id)
                                                and sent < len(self.requests))
                            sent = max(sent, len(self.replies) if self.mode == "passive" else 0)
                            if sent >= len(self.requests):
                                continue
                            request = self.requests[sent]
                            number = request["request_num"]
                            pending[number] = asyncio.get_running_loop().time()
                            await asyncio.wait_for(ws.send(json.dumps(request)),
                                                   self.args.timeout)
                            sent = number
                            self.sent[replica_id] = number
                            self.changed.set()
                            log(
                                f"{self.args.client_id}: Sent "
                                f"<{self.args.client_id}, {replica_id}, {number}, request>",
                                "request",
                            )

                    async def receive_replies():
                        while True:
                            raw = await ws.recv()
                            reply = json.loads(raw)
                            request_num = reply.get("request_num")
                            if (type(request_num) is not int
                                    or request_num not in pending):
                                raise ValueError(f"unexpected reply from {replica_id}")
                            request = self.requests[request_num - 1]
                            if (reply.get("type") != "reply" or
                                    reply.get("client_id") != self.args.client_id or
                                    reply.get("replica_id") != replica_id or
                                    reply.get("epoch") != self.epoch or
                                    reply.get("global_seq") != request["global_seq"] or
                                    reply.get("my_state") != request["global_seq"]):
                                raise ValueError(f"invalid reply from {replica_id}")
                            del pending[request_num]
                            log(
                                f"{self.args.client_id}: Received "
                                f"<{self.args.client_id}, {replica_id}, "
                                f"{request_num}, reply> "
                                f"server_state={reply['my_state']}",
                                "reply",
                            )
                            if self.mode == "passive" and replica_id != self.primary_id:
                                log(f"{self.args.client_id}: Ignored reply from former "
                                    f"primary {replica_id}", "fault")
                            elif request_num in self.replies:
                                log(f"{self.args.client_id}: request_num {request_num}: "
                                    f"Discarded duplicate reply from {replica_id}", "duplicate")
                            else:
                                self.replies[request_num] = reply
                                log(f"{self.args.client_id}: request_num "
                                    f"{request_num}: Delivered reply", "reply")
                            self.changed.set()

                    async def watch_replies():
                        while True:
                            await asyncio.sleep(min(self.args.timeout / 2, 0.5))
                            if (pending and self.can_reply(replica_id)
                                    and asyncio.get_running_loop().time() - min(pending.values())
                                    >= self.args.timeout):
                                raise TimeoutError("replica did not reply")

                    tasks = [
                        asyncio.create_task(send_requests()),
                        asyncio.create_task(receive_replies()),
                        asyncio.create_task(watch_replies()),
                    ]
                    done, _ = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in done:
                        task.result()
            except (OSError, TimeoutError, ValueError, WebSocketException) as exc:
                log(f"{self.args.client_id}: {replica_id} connection lost "
                    f"({type(exc).__name__}: {exc})", "fault")
                await asyncio.sleep(0.3)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def produce_requests(self):
        request_num = 1
        while not self.args.max_requests or request_num <= self.args.max_requests:
            while request_num not in self.orders:
                await self.wait_for(lambda: self.control_ws is not None and self.available())
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
            await self.wait_for(lambda: request_num in self.replies and (
                self.mode != "active" or all(self.sent.get(replica_id, 0) >= request_num
                                             for replica_id in self.ready_members())
            ))
            request_num += 1
            await asyncio.sleep(self.args.interval)
        if self.mode == "active":
            await asyncio.sleep(self.args.drain_seconds)

    async def run(self):
        self.failure = asyncio.get_running_loop().create_future()
        tasks = [
            asyncio.create_task(self.gfd_connection()),
            asyncio.create_task(self.produce_requests()),
        ]
        try:
            done, _ = await asyncio.wait([*tasks, self.failure],
                                         return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in [*tasks, *self.connections.values()]:
                task.cancel()
            await asyncio.gather(
                *tasks,
                *self.connections.values(),
                return_exceptions=True,
            )
            self.failure.cancel()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client_id", required=True)
    parser.add_argument("--gfd_url", default="ws://127.0.0.1:9000")
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--max_requests", type=int, default=0)
    parser.add_argument("--drain_seconds", type=float, default=1)
    args = parser.parse_args()
    if min(args.interval, args.max_requests, args.drain_seconds) < 0 or args.timeout <= 0:
        parser.error("invalid timing or request-count value")
    return args


if __name__ == "__main__":
    try:
        asyncio.run(ReplicatedClient(parse_args()).run())
    except KeyboardInterrupt:
        pass
