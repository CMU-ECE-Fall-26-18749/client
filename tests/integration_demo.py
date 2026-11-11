"""Exercise the real clients and LFDs through server crashes and recovery."""

from contextlib import ExitStack
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
sys.path.insert(0, str(SERVER / "tests"))

from milestone4_demo import health, unused_ports, wait_for


def run(mode):
    output = SERVER / "test-results" / f"milestone4-clients-{mode}"
    output.mkdir(parents=True, exist_ok=True)
    rm_port, gfd_port, *ports = unused_ports(5)
    gfd_url = f"ws://127.0.0.1:{gfd_port}"
    clients = ("C1", "C2", "C3")

    with ExitStack() as cleanup:
        def stop(process):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

        def start(name, script, *flags):
            stream = cleanup.enter_context((output / f"{name}.log").open("w", encoding="utf-8"))
            process = subprocess.Popen([sys.executable, str(script), *map(str, flags)],
                                       cwd=script.parent, stdout=stream,
                                       stderr=subprocess.STDOUT)
            cleanup.callback(stop, process)
            return process

        def log(name):
            return (output / f"{name}.log").read_text(encoding="utf-8")

        def members():
            return {member["replica_id"]: member for member in health(gfd_port)["members"]}

        def launch(number, suffix=""):
            return start(f"S{number}{suffix}", SERVER / "replica.py",
                         "--replica_id", f"S{number}", "--port", ports[number - 1],
                         "--mode", mode, "--gfd_url", gfd_url, "--checkpoint_interval", 0.2)

        def progress(label):
            before = {name: log(name).count("Delivered reply") for name in clients}
            wait_for(lambda: all(log(name).count("Delivered reply") >= before[name] + 4
                                 for name in clients), label)

        start("RM", SERVER / "rm.py", "--port", rm_port)
        start("GFD", SERVER / "gfd.py", "--port", gfd_port, "--mode", mode,
              "--rm_url", f"ws://127.0.0.1:{rm_port}", "--heartbeat_freq", 0.1)
        replicas = []
        for number, port in enumerate(ports, 1):
            start(f"LFD{number}", ROOT / "LFD/lfd.py",
                  "--lfd_id", f"LFD{number}", "--replica_id", f"S{number}",
                  "--server_url", f"ws://127.0.0.1:{port}", "--gfd_url", gfd_url,
                  "--heartbeat_freq", 0.1, "--timeout", 0.5)
            replicas.append(launch(number))
            wait_for(lambda: members().get(f"S{number}", {}).get("ready"), f"S{number} ready")
        for client in clients:
            start(client, ROOT / "client/client.py", "--client_id", client,
                  "--gfd_url", gfd_url, "--interval", 0.04, "--timeout", 1)
        progress("initial replies")
        if mode == "active":
            wait_for(lambda: all("Discarded duplicate reply" in log(name) for name in clients),
                     "duplicate detection")

        stop(replicas[1])
        wait_for(lambda: "S2" not in members(), "S2 removed")
        progress("replies after S2 crash")
        launch(2, "-restarted")
        wait_for(lambda: members().get("S2", {}).get("ready"), "S2 recovered")
        assert "Detected restart of S2" in log("LFD2")
        progress("replies after S2 recovery")
        if mode == "active":
            wait_for(lambda: all(f"Received <{name}, S2," in
                                 log(name).rsplit("Connected to S2", 1)[-1]
                                 for name in clients), "replies from the restarted S2")
        else:
            stop(replicas[0])
            wait_for(lambda: health(gfd_port)["primary_id"] == "S2", "S2 promoted")
            progress("replies after S1 crash")
            assert all(f"Received <{name}, S2," in log(name) for name in clients)

        for name in clients:
            lines = log(name).splitlines()
            delivered = [line.split("request_num ")[-1].split(":")[0]
                         for line in lines if "Delivered reply" in line]
            assert list(map(int, delivered)) == list(range(1, len(delivered) + 1)), name
            assert "Traceback" not in log(name)
    print(f"Milestone 4 {mode} with real clients: PASS", flush=True)


if __name__ == "__main__":
    run("active")
    run("passive")
