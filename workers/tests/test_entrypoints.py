from graphx_workers.config import WorkerKind
from graphx_workers.runtime.application import run


def test_check_mode_validates_each_worker_kind() -> None:
    for kind in WorkerKind:
        assert run(kind, ["--check"]) == 0
