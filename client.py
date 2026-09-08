"""
Client (C1 / C2 / C3) — 18-749 Milestone 1

Usage:
    python client.py --client_id C1 --server_url ws://localhost:8001
    python client.py --client_id C2 --server_url ws://localhost:8001
    python client.py --client_id C3 --server_url ws://localhost:8001

Sends requests in a continuous loop to the server replica.
Each request contains <client_id, replica_id, request_num>.
"""

import argparse
import asyncio
import json

import websockets

from utils import log

parser = argparse.ArgumentParser(description="Client")
parser.add_argument("--client_id", type=str, required=True, help="e.g. C1, C2, C3")
parser.add_argument(
    "--server_url",
    type=str,
    default="ws://localhost:8001",
    help="WebSocket base URL of the server replica",
)
parser.add_argument(
    "--interval",
    type=float,
    default=2.0,
    help="Seconds between requests (default: 2.0)",
)
args = parser.parse_args()

CLIENT_ID = args.client_id
SERVER_URL = args.server_url
INTERVAL = args.interval


async def run_client():
    """Connect to the server and send requests in a continuous loop."""
    url = f"{SERVER_URL}/ws/client/{CLIENT_ID}"
    request_num = 1

    while True:
        try:
            async with websockets.connect(url) as ws:
                log(f"{CLIENT_ID}: Connected to server at {SERVER_URL}", "info")

                while True:
                    # Build and send request
                    msg = {
                        "client_id": CLIENT_ID,
                        "request_num": request_num,
                        "payload": f"request_{request_num}_from_{CLIENT_ID}",
                    }
                    await ws.send(json.dumps(msg))
                    log(
                        f"{CLIENT_ID}: Sent <{CLIENT_ID}, server, {request_num}, request>",
                        "request",
                    )

                    # Wait for reply
                    raw = await ws.recv()
                    reply = json.loads(raw)
                    r_id = reply.get("replica_id", "?")
                    log(
                        f"{CLIENT_ID}: Received <{CLIENT_ID}, {r_id}, {request_num}, reply> "
                        f"server_state={reply.get('my_state', '?')}",
                        "reply",
                    )

                    request_num += 1
                    await asyncio.sleep(INTERVAL)

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
            log(
                f"{CLIENT_ID}: Connection to server lost ({type(e).__name__}). "
                f"Retrying in 3s...",
                "fault",
            )
            await asyncio.sleep(3)


if __name__ == "__main__":
    log(f"{CLIENT_ID}: Starting client, targeting {SERVER_URL}", "info")
    asyncio.run(run_client())