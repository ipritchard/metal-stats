"""JSON and NDJSON checkpoint helpers with resume support."""

import json
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON checkpoint file.

    Args:
        path: Path to JSON file.

    Returns:
        Parsed dict, or empty dict if file missing/empty.
    """
    if not path.exists() or path.stat().st_size == 0:
        return {}
    return json.loads(path.read_text())


def write_json(path: Path, data: dict[str, Any]) -> None:
    """Write data to a JSON checkpoint file atomically.

    Args:
        path: Destination path.
        data: Dict to serialize.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    tmp.rename(path)


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    """Read all records from an NDJSON file.

    Args:
        path: Path to NDJSON file.

    Returns:
        List of parsed dicts, or empty list if file missing.
    """
    if not path.exists():
        return []
    records = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def append_ndjson(path: Path, record: dict[str, Any]) -> None:
    """Append a single record to an NDJSON file.

    Args:
        path: Destination NDJSON file.
        record: Dict to serialize and append.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def ndjson_ids(path: Path, key: str = "id") -> set[str]:
    """Extract the set of IDs already present in an NDJSON checkpoint.

    Args:
        path: Path to NDJSON file.
        key: Field name to use as ID.

    Returns:
        Set of ID strings already recorded.
    """
    return {r[key] for r in read_ndjson(path) if key in r}
