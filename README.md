# 18-749 client

Milestone 2 client with GFD membership, parallel replica connections, globally
ordered requests, first-reply delivery, duplicate suppression, and retry replay.

Keep `client`, `server`, and `LFD` as sibling folders. From their parent:

```text
python server/demo.py setup
python server/demo.py c1
```

Use separate terminals for `c2` and `c3`. Start the GFD, LFDs, and all three replicas
first. The coordinator defaults to localhost. For another coordinator, use
`--gfd-host HOST` with the launcher, or run:

```text
python client.py --client_id C1 --gfd_url ws://HOST:9000 --interval 1
```

See the sibling server repository's README for the four-machine demo, design,
limitations, tests, and rollback instructions. Legacy single-server mode uses
`--server_url ws://HOST:8001` instead of `--gfd_url`.
