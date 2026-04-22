"""HansolCal 엔트리포인트.

사용:
  python main.py                # config.yaml 로드 후 1회 동기화
  python main.py --config path  # 다른 config 파일
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from config import load_config
from gcal_api import GCalAPI
from notion_api import NotionAPI
from sync import SyncEngine


def _load_env_file(path: str = ".env") -> None:
    """.env 파일 있으면 로드 (GitHub Actions에선 없으니 no-op)."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def main() -> int:
    _load_env_file()
    parser = argparse.ArgumentParser(description="HansolCal: Notion ↔ Google Calendar 2-way sync")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)

    logging.basicConfig(
        level=getattr(logging, config.options.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("hansolcal")
    log.info(f"Loaded {len(config.mappings)} mapping(s); dry_run={config.options.dry_run}")

    notion = NotionAPI()
    gcal = GCalAPI()
    engine = SyncEngine(config, notion, gcal)
    stats = engine.run()

    total_errors = sum(s.errors for s in stats.values())
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
