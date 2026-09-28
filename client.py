"""Continuous client; --gfd_url enables active replication and duplicate suppression."""

import argparse
import asyncio
import contextlib
import json

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from utils import log


class ActiveClient:
    def __init__(self, args):
        self.args = args
        self.epoch = None
        self.version = -1
        self.members = {}
        self.workers = {}
        self.sent = {}
        self.history = []
        self.replies = {}
        self.orders = {}
        self.control_ws = None
        self.changed = asyncio.Event()
        self.failure = None

    async def wait_until(self, predicate):
        while not predicate():
            self.changed.clear()
            if not predicate():
                await self.changed.wait()

    def worker_done(self, task):
        if not task.cancelled() and task.exception() and not self.failure.done():
            self.failure.set_exception(task.exception())

    async def membership(self, msg):
        if self.epoch is not None and self.epoch != msg["epoch"]:
            raise RuntimeError("GFD restarted: stop and restart the whole demo")
        self.epoch = msg["epoch"]
        if msg["version"] <= self.version:
            return
        self.version = msg["version"]
        current = {m["replica_id"]: m for m in msg["members"]}
        for rid in list(self.workers):
            if rid not in current or current[rid] != self.members[rid]:
                task = self.workers.pop(rid)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
                self.sent.pop(rid, None)
        self.members = current
        for rid, member in current.items():
            if rid not in self.workers:
                self.sent[rid] = 0
                task = asyncio.create_task(self.replica(member))
                task.add_done_callback(self.worker_done)
                self.workers[rid] = task
        log(f"{self.args.client_id}: Received GFD membership v{self.version}: "
            f"{', '.join(sorted(current)) or '(empty)'}", "membership")
        self.changed.set()

    async def control(self):
        while True:
            try:
                url = self.args.gfd_url.rstrip("/") + f"/ws/client/{self.args.client_id}"
                async with connect(url, open_timeout=3, close_timeout=1, proxy=None) as ws:
                    self.control_ws = ws
                    self.changed.set()
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg["type"] == "membership":
                            await self.membership(msg)
                        elif msg["type"] == "ordered":
                            self.orders[msg["request_num"]] = msg
                            self.changed.set()
                        elif msg["type"] == "unavailable":
                            log(f"{self.args.client_id}: No healthy replicas; waiting", "fault")
            except (OSError, TimeoutError, WebSocketException) as exc:
                log(f"{self.args.client_id}: GFD connection lost ({type(exc).__name__})", "fault")
            finally:
                self.control_ws = None
                self.changed.set()
            await asyncio.sleep(0.5)

    async def replica(self, member):
        rid = member["replica_id"]
        acknowledged = 0
        while True:
            tasks = []
            try:
                url = member["url"].rstrip("/") + f"/ws/client/{self.args.client_id}"
                async with connect(url, open_timeout=2, close_timeout=1, proxy=None) as ws:
                    log(f"{self.args.client_id}: Connected to {rid}", "info")

                    async def send_requests():
                        index = acknowledged
                        while True:
                            await self.wait_until(lambda: index < len(self.history))
                            msg = self.history[index]
                            req = msg["request_num"]
                            await asyncio.wait_for(ws.send(json.dumps(msg)), self.args.timeout)
                            self.sent[rid] = req
                            self.changed.set()
                            log(f"{self.args.client_id}: Sent <{self.args.client_id}, {rid}, "
                                f"{req}, request> global_seq={msg['global_seq']}", "request")
                            index += 1

                    async def receive_replies():
                        nonlocal acknowledged
                        while True:
                            await self.wait_until(lambda: acknowledged < len(self.history))
                            reply = json.loads(await asyncio.wait_for(ws.recv(), self.args.timeout))
                            req = reply.get("request_num")
                            if type(req) is not int or not 1 <= req <= len(self.history):
                                raise RuntimeError(f"Invalid request identity in reply from {rid}: {reply}")
                            msg = self.history[req - 1]
                            if (reply.get("type") != "reply" or
                                    reply.get("client_id") != self.args.client_id or
                                    reply.get("replica_id") != rid or reply.get("epoch") != self.epoch or
                                    reply.get("global_seq") != msg["global_seq"] or
                                    reply.get("my_state") != msg["global_seq"] or req > acknowledged + 1):
                                raise RuntimeError(f"Invalid/inconsistent reply from {rid}: {reply}")
                            log(f"{self.args.client_id}: Received <{self.args.client_id}, {rid}, "
                                f"{req}, reply> server_state={reply['my_state']}", "reply")
                            if req in self.replies:
                                log(f"{self.args.client_id}: request_num {req}: "
                                    f"Discarded duplicate reply from {rid}", "duplicate")
                            else:
                                self.replies[req] = reply
                                log(f"{self.args.client_id}: request_num {req}: "
                                    f"Delivered first reply from {rid}", "reply")
                            acknowledged = max(acknowledged, req)
                            self.changed.set()

                    tasks = [asyncio.create_task(send_requests()), asyncio.create_task(receive_replies())]
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
            except (OSError, TimeoutError, WebSocketException) as exc:
                log(f"{self.args.client_id}: {rid} connection lost ({type(exc).__name__}); "
                    "other replicas remain independent", "fault")
                await asyncio.sleep(0.2)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def produce(self):
        req = 1
        while not self.args.max_requests or req <= self.args.max_requests:
            await self.wait_until(lambda: self.control_ws is not None and bool(self.members))
            while req not in self.orders:
                try:
                    await asyncio.wait_for(self.control_ws.send(json.dumps({
                        "type": "order", "request_num": req,
                        "payload": f"request_{req}_from_{self.args.client_id}"})), self.args.timeout)
                    await asyncio.wait_for(self.wait_until(lambda: req in self.orders), self.args.timeout)
                except (AttributeError, OSError, TimeoutError, WebSocketException):
                    await asyncio.sleep(0.2)
                    await self.wait_until(lambda: self.control_ws is not None and bool(self.members))
            self.history.append(self.orders[req])
            self.changed.set()
            # Send this request to every currently healthy member before the next.
            # Membership removal releases a wait on a failed replica; other workers keep running.
            await self.wait_until(lambda: req in self.replies and
                                  all(self.sent.get(rid, 0) >= req for rid in self.members))
            req += 1
            await asyncio.sleep(self.args.interval)
        await asyncio.sleep(self.args.drain_seconds)

    async def run(self):
        self.failure = asyncio.get_running_loop().create_future()
        tasks = [asyncio.create_task(self.control()), asyncio.create_task(self.produce())]
        try:
            done, _ = await asyncio.wait([*tasks, self.failure], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in [*tasks, *self.workers.values()]:
                task.cancel()
            await asyncio.gather(*tasks, *self.workers.values(), return_exceptions=True)
            if not self.failure.done():
                self.failure.cancel()


async def standalone(args):
    req = 1
    while not args.max_requests or req <= args.max_requests:
        try:
            async with connect(args.server_url.rstrip("/") + f"/ws/client/{args.client_id}",
                               open_timeout=3, close_timeout=1, proxy=None) as ws:
                while not args.max_requests or req <= args.max_requests:
                    await ws.send(json.dumps({"client_id": args.client_id,
                        "request_num": req, "payload": f"request_{req}_from_{args.client_id}"}))
                    log(f"{args.client_id}: Sent <{args.client_id}, server, {req}, request>", "request")
                    reply = json.loads(await asyncio.wait_for(ws.recv(), args.timeout))
                    log(f"{args.client_id}: Received <{args.client_id}, {reply['replica_id']}, "
                        f"{reply['request_num']}, reply> server_state={reply['my_state']}", "reply")
                    req += 1
                    await asyncio.sleep(args.interval)
        except (OSError, TimeoutError, WebSocketException) as exc:
            log(f"{args.client_id}: Connection lost ({type(exc).__name__}); retrying", "fault")
            await asyncio.sleep(1)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client_id", required=True)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--gfd_url", help="Milestone 2: ws://GFD_HOST:9000")
    target.add_argument("--server_url", default="ws://127.0.0.1:8001",
                        help="Milestone 1 single-server mode")
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--max_requests", type=int, default=0, help="0 runs continuously")
    parser.add_argument("--drain_seconds", type=float, default=1, help="Drain late replies in finite runs")
    args = parser.parse_args()
    if args.interval < 0 or args.timeout <= 0 or args.max_requests < 0 or args.drain_seconds < 0:
        parser.error("interval/drain_seconds/max_requests must be nonnegative; timeout must be positive")
    return args


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(ActiveClient(args).run() if args.gfd_url else standalone(args))
    except KeyboardInterrupt:
        pass
