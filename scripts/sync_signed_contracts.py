#!/usr/bin/env python3
"""Run signed-contract sync (flags only; never changes stage)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

load_dotenv(_BACKEND_ROOT / ".env")

from database import SessionLocal, init_db  # noqa: E402
from services.signed_contract_dry_run import recompute_signed_contracts  # noqa: E402


def main() -> int:
    init_db()
    db = SessionLocal()
    try:
        result = recompute_signed_contracts(db)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    finally:
        db.close()

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
