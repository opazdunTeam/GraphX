from graphx_workers.config import WorkerKind
from graphx_workers.runtime.application import run


def main() -> int:
    return run(WorkerKind.SOURCE)


if __name__ == "__main__":
    raise SystemExit(main())
