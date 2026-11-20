"""Run the RabbitMQ integration suite in an isolated Docker Compose environment."""

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.test.yaml"
TEST_URL = "amqp://test_user:test_password@127.0.0.1:5673/graphx_test"


def run(*command: str, check: bool = True, env: dict[str, str] | None = None) -> int:
    return subprocess.run(command, cwd=ROOT, check=check, env=env).returncode


def main() -> int:
    compose = ("docker", "compose", "-f", str(COMPOSE_FILE))
    try:
        run(*compose, "up", "-d", "--wait", "rabbitmq-test")
        run(*compose, "--profile", "image-smoke", "up", "-d", "--build", "source-worker-smoke")
        time.sleep(2)
        container_id = subprocess.run(
            (*compose, "--profile", "image-smoke", "ps", "-q", "source-worker-smoke"),
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not container_id:
            raise RuntimeError("source worker smoke container was not created")
        state = subprocess.run(
            ("docker", "inspect", "--format", "{{.State.Status}}", container_id),
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if state != "running":
            run(*compose, "--profile", "image-smoke", "logs", "source-worker-smoke", check=False)
            raise RuntimeError(f"source worker smoke container is {state}")
        run(
            "docker",
            "exec",
            container_id,
            "python",
            "-c",
            (
                "from datetime import timedelta; from pathlib import Path; import portalocker; "
                "from graphx_workers.runtime.replay import FileTerminalReplayStore; "
                "root=Path('/tmp/claim-inode-smoke'); root.mkdir(exist_ok=True); "
                "path=root/'claim.lock'; old=portalocker.Lock(path,mode='a+b',timeout=0); "
                "old.acquire(); path.unlink(); "
                "current=portalocker.Lock(path,mode='a+b',timeout=0); "
                "current.acquire(); store=FileTerminalReplayStore(root,retention=timedelta(days=1),"
                "cleanup_interval=timedelta(minutes=1),max_entries=2,max_bytes=8192,"
                "max_entry_bytes=4096); assert not store._claim_is_current(old,path); "
                "assert store._claim_is_current(current,path); current.release(); old.release()"
            ),
        )
        environment = os.environ.copy()
        environment["RABBITMQ_TEST_URL"] = TEST_URL
        run(
            sys.executable,
            "tools/container_smoketest.py",
            env=environment,
        )
        run(
            "docker",
            "exec",
            container_id,
            "python",
            "-c",
            (
                "from pathlib import Path; "
                "root=Path('/var/lib/graphx/worker-replay'); "
                "assert len(tuple(root.glob('[0-9a-f]*.json'))) == 1"
            ),
        )
        run(*compose, "--profile", "image-smoke", "stop", "source-worker-smoke")
        return run(
            sys.executable,
            "-m",
            "pytest",
            "-m",
            "integration",
            env=environment,
            check=False,
        )
    finally:
        run(
            *compose,
            "--profile",
            "image-smoke",
            "down",
            "-v",
            "--remove-orphans",
            check=False,
        )


if __name__ == "__main__":
    raise SystemExit(main())
