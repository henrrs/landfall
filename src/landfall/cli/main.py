"""landfall CLI."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from landfall import __version__
from landfall.adapters.loaders import load_contract, load_platform, resolve_platform
from landfall.adapters.runtime import local_capacity
from landfall.adapters.secrets import make_secret_provider
from landfall.adapters.sqlserver import SqlServerConnector
from landfall.adapters.storage import make_storage
from landfall.application.capture import CaptureEngine


def _engine(args) -> CaptureEngine:
    contract = load_contract(args.contract)
    platform = load_platform(args.platform)
    account, container, policy = resolve_platform(
        platform, contract.document["metadata"]["domain"],
        contract.document["metadata"]["sourceSystem"])
    if contract.document["source"]["connector"] != "sqlserver":
        raise ValueError(f"no connector for {contract.document['source']['connector']}")
    secret = make_secret_provider(platform).resolve(contract.document["source"]["connectionRef"])
    source = SqlServerConnector(secret, platform.get("sqlserver", {})).bind(contract)
    storage = make_storage(platform, account, container)
    return CaptureEngine(contract, source, storage, local_capacity(), policy,
                         platform.get("landing", {}).get("compression", "lz4"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="landfall")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version")
    for command in ("run", "plan", "inspect", "validate-contract"):
        cmd = sub.add_parser(command)
        cmd.add_argument("--contract")
        if command in {"run", "plan", "inspect"}:
            cmd.add_argument("--platform")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    logging.getLogger("azure").setLevel(logging.WARNING)
    try:
        if args.command == "version":
            print(__version__)
        elif args.command == "validate-contract":
            contract = load_contract(args.contract)
            print(json.dumps({"identity": contract.identity, "version": contract.version,
                              "fingerprint": contract.hash}))
        else:
            engine = _engine(args)
            if args.command == "inspect":
                engine.source.validate(engine.contract)
                print(json.dumps({"identity": engine.contract.identity, "version": engine.contract.version,
                                  "source": engine.contract.document["source"]["connector"],
                                  "landing": engine.base, "state": engine._state()[0]}, default=str, indent=2))
            elif args.command == "plan":
                _, _, physical = engine.prepare()
                print(json.dumps(physical.to_dict(), default=str, indent=2))
            else:
                print(json.dumps(engine.run(), default=str, indent=2))
    except Exception as exc:  # noqa: BLE001 - CLI returns a concise error for adapter failures
        print(f"landfall: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
