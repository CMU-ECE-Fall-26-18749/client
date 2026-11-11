# Milestone 4 Client

The client learns the replication mode and addresses from the GFD. In active
mode it sends each request to the replicas, delivers one reply, and logs the
duplicates. It does not wait for a recovering replica before advancing.
In passive mode it sends requests only to the announced primary and retries
an outstanding request with the same ID when a backup takes over.

From the directory containing the three repositories, run one of these
commands for each client, replacing `C1` with `C2` or `C3` and replacing
`GFD_IP` with the coordinator address. The command is the same in both modes.

```sh
.venv/bin/python client/client.py --client_id C1 --gfd_url ws://GFD_IP:9000
```

Run `.venv/bin/python client/tests/integration_demo.py` to exercise all three
clients and LFDs through active recovery and passive failover locally.
