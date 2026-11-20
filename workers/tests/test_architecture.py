import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "src" / "graphx_workers"
FORBIDDEN_INFRASTRUCTURE = {
    "aio_pika",
    "aiormq",
    "asyncpg",
    "boto3",
    "botocore",
    "minio",
    "pika",
    "psycopg",
}


def test_sdk_and_source_adapters_do_not_import_transport_or_database_clients() -> None:
    violations: list[str] = []
    for package in (ROOT / "sdk", ROOT / "sources"):
        for path in package.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                imported: list[str] = []
                if isinstance(node, ast.Import):
                    imported = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    imported = [node.module]
                for module in imported:
                    if module.split(".", maxsplit=1)[0] in FORBIDDEN_INFRASTRUCTURE:
                        violations.append(f"{path.relative_to(ROOT)} imports {module}")

    assert not violations, "\n".join(violations)
