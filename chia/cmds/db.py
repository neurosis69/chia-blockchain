from __future__ import annotations

from pathlib import Path
from typing import Optional

import click

from chia.cmds.db_backup_func import db_backup_func
from chia.cmds.db_upgrade_func import db_upgrade_func
from chia.cmds.db_upgrade_alt_func import db_upgrade_alt_func
from chia.cmds.db_validate_func import db_validate_func


@click.group("db", help="Manage the blockchain database")
def db_cmd() -> None:
    pass


@db_cmd.command("upgrade", help="upgrade a v1 database to v2")
@click.option("--input", "in_db_path", default=None, type=click.Path(), help="specify input database file")
@click.option("--output", "out_db_path", default=None, type=click.Path(), help="specify output database file")
@click.option(
    "--no-update-config",
    default=False,
    is_flag=True,
    help="don't update config file to point to new database. When specifying a "
    "custom output file, the config will not be updated regardless",
)
@click.option(
    "--force",
    default=False,
    is_flag=True,
    help="force conversion despite warnings",
)
@click.pass_context
def db_upgrade_cmd(
    ctx: click.Context,
    in_db_path: Optional[str],
    out_db_path: Optional[str],
    no_update_config: bool,
    force: bool,
) -> None:
    try:
        db_upgrade_func(
            Path(ctx.obj["root_path"]),
            None if in_db_path is None else Path(in_db_path),
            None if out_db_path is None else Path(out_db_path),
            no_update_config=no_update_config,
            force=force,
        )
    except RuntimeError as e:
        print(f"FAILED: {e}")


@db_cmd.command("upgrade_alt", short_help="EXPERIMENTAL: upgrade v1 database to v2 - alternative approach")
@click.option("--input", default=None, type=click.Path(), help="specify input database file")
@click.option("--output", default=None, type=click.Path(), help="specify output database file")
@click.option("--hdd", default=False, is_flag=True, help="select if database is located on HDD device. (DEFAULT=false)")
@click.option(
    "--offline",
    default=False,
    is_flag=True,
    help="select to upgrade database with no active full node. (DEFAULT=false)",
)
@click.option(
    "--temp-store-path",
    default=None,
    type=click.Path(),
    help="specify path used as temporary db destination (Linux only)",
)
@click.option(
    "--check-only",
    default=False,
    is_flag=True,
    help="specify to check only if enough disk space is available for upgrade. (DEFAULT=false)",
)
@click.option(
    "--no-update-config",
    default=False,
    is_flag=True,
    help="don't update config file to point to new database. When specifying a "
    "custom output file, the config will not be updated regardless",
)
@click.pass_context
def db_upgrade_alt_cmd(
    ctx: click.Context, no_update_config: bool, offline: bool, hdd: bool, check_only: bool, **kwargs
) -> None:

    in_db_path = kwargs.get("input")
    out_db_path = kwargs.get("output")
    temp_path = kwargs.get("temp_store_path")
    db_upgrade_alt_func(
        Path(ctx.obj["root_path"]),
        None if in_db_path is None else Path(in_db_path),
        None if out_db_path is None else Path(out_db_path),
        no_update_config,
        offline,
        hdd,
        None if temp_path is None else Path(temp_path),
        check_only,
    )
@db_cmd.command("validate", help="validate the (v2) blockchain database. Does not verify proofs")
@click.option("--db", "in_db_path", default=None, type=click.Path(), help="Specifies which database file to validate")
@click.option(
    "--validate-blocks",
    default=False,
    is_flag=True,
    help="validate consistency of properties of the encoded blocks and block records",
)
@click.pass_context
def db_validate_cmd(ctx: click.Context, in_db_path: Optional[str], validate_blocks: bool) -> None:
    try:
        db_validate_func(
            Path(ctx.obj["root_path"]),
            None if in_db_path is None else Path(in_db_path),
            validate_blocks=validate_blocks,
        )
    except RuntimeError as e:
        print(f"FAILED: {e}")


@db_cmd.command("backup", help="backup the blockchain database using VACUUM INTO command")
@click.option("--backup_file", "db_backup_file", default=None, type=click.Path(), help="Specifies the backup file")
@click.option("--no_indexes", default=False, is_flag=True, help="Create backup without indexes")
@click.pass_context
def db_backup_cmd(ctx: click.Context, db_backup_file: Optional[str], no_indexes: bool) -> None:
    try:
        db_backup_func(
            Path(ctx.obj["root_path"]),
            None if db_backup_file is None else Path(db_backup_file),
            no_indexes=no_indexes,
        )
    except RuntimeError as e:
        print(f"FAILED: {e}")
