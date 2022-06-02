from pathlib import Path
from typing import Any, Dict, Optional

from chia.consensus.block_record import BlockRecord
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.full_block import FullBlock
from chia.util.config import load_config
from chia.util.path import path_from_root


def db_backup_func(
    root_path: Path,
    backup_db_file: Path,
) -> None:
    config: Dict[str, Any] = load_config(root_path, "config.yaml")["full_node"]
    selected_network: str = config["selected_network"]
    db_pattern: str = config["database_path"]
    db_path_replaced: str = db_pattern.replace("CHALLENGE", selected_network)
    source_db = path_from_root(root_path, db_path_replaced)

    backup_db(source_db, backup_db_file)

    print(f"\n\nDatabase backup finished : {backup_db_file}\n")


def backup_db(source_db: Path, backup_db: Path) -> None:
    import sqlite3
    from contextlib import closing

    if not backup_db.parent.exists():
        print(f"backup destination path doesn't exist. {backup_db.parent}")
        raise RuntimeError(f"can't find {backup_db}")

    print(f"reading from blockchain database: {source_db}")
    print(f"writing to backup file: {backup_db}")
    with closing(sqlite3.connect(source_db)) as in_db:
        try:
            in_db.execute("VACUUM INTO ?", [str(backup_db)])
        except sqlite3.OperationalError:
            raise RuntimeError("Database backup not finished successfully")
