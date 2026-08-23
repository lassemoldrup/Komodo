from __future__ import annotations

import hashlib
import tarfile
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Iterator


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


@contextmanager
def archived_bundle(root: Path, bundle: dict[str, object]) -> Iterator[Path]:
    purpose = bundle.get("purpose")
    archive_name = bundle.get("archive_path")
    archive_sha256 = bundle.get("archive_sha256")
    require(isinstance(archive_name, str) and archive_name,
            f"evidence archive path is missing: {purpose}")
    require(
        isinstance(archive_sha256, str)
        and len(archive_sha256) == 64
        and all(character in "0123456789abcdef" for character in archive_sha256),
        f"evidence archive digest is malformed: {purpose}",
    )
    archive = root / archive_name
    require(archive.is_file(), f"evidence archive is missing: {archive}")
    require(digest(archive) == archive_sha256,
            f"evidence archive digest differs: {purpose}")

    bundle_path = bundle.get("bundle_path")
    require(isinstance(bundle_path, str) and bundle_path,
            f"evidence bundle path is missing: {purpose}")
    expected_root = Path(bundle_path).name
    with tempfile.TemporaryDirectory(prefix="yugabytedb-ysql-evidence-") as temporary:
        extraction_root = Path(temporary)
        with tarfile.open(archive, mode="r:gz") as source:
            members = source.getmembers()
            require(bool(members), f"evidence archive is empty: {purpose}")
            for member in members:
                name = PurePosixPath(member.name)
                require(
                    not name.is_absolute()
                    and ".." not in name.parts
                    and name.parts
                    and name.parts[0] == expected_root,
                    f"evidence archive member escapes its bundle root: {member.name}",
                )
                require(
                    member.isfile() or member.isdir(),
                    f"evidence archive contains a non-regular member: {member.name}",
                )
            source.extractall(extraction_root, filter="data")
        directory = extraction_root / expected_root
        require(directory.is_dir(), f"evidence archive bundle root is missing: {purpose}")
        yield directory
