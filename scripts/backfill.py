"""Backfill the warehouse over a date range, one hour at a time.

Usage: python scripts/backfill.py 2024-01-01 2024-01-07

Iterates hourly from start (inclusive) to end (inclusive). Failed hours print to
stderr and are skipped, so you can re-run just the range that failed.
"""

import sys
from datetime import datetime, timedelta

from ghpulse.pipeline import run_hour


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2

    start = datetime.fromisoformat(argv[1])
    end = datetime.fromisoformat(argv[2])

    current = start
    failures = 0
    while current <= end:
        try:
            result = run_hour(current)
            print(f"OK  {current.isoformat()}  {result['rows_written']:,} rows")
        except Exception as e:
            print(f"ERR {current.isoformat()}  {type(e).__name__}: {e}", file=sys.stderr)
            failures += 1
        current += timedelta(hours=1)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
