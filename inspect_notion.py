"""Notion DB 스키마 검사 — config.yaml 작성 전에 property 이름 확인용.

사용:
  export NOTION_TOKEN='ntn_xxxxx'
  python inspect_notion.py <database_id>
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from notion_client import Client


def _load_env_file(path: str = ".env") -> None:
    """.env 파일이 있으면 환경변수로 로드 (이미 설정된 건 건드리지 않음)."""
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
    if len(sys.argv) != 2:
        print("Usage: python inspect_notion.py <database_id>", file=sys.stderr)
        return 1
    db_id = sys.argv[1]
    _load_env_file()
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        print("ERROR: set NOTION_TOKEN env var first", file=sys.stderr)
        return 1

    client = Client(auth=token)
    try:
        db = client.databases.retrieve(database_id=db_id)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        print("  → integration이 이 DB에 연결(Connections에 추가)되었는지 확인하세요.", file=sys.stderr)
        return 1

    title_arr = db.get("title", [])
    db_title = "".join(t.get("plain_text", "") for t in title_arr)
    print(f"Database: {db_title}")
    print(f"ID:       {db['id']}")
    print("")
    print("Properties:")
    for name, info in db.get("properties", {}).items():
        t = info.get("type", "?")
        extra = ""
        if t == "status":
            opts = info.get("status", {}).get("options", [])
            extra = " options=[" + ", ".join(f"{o['name']}({o['color']})" for o in opts) + "]"
        elif t == "select":
            opts = info.get("select", {}).get("options", [])
            extra = " options=[" + ", ".join(f"{o['name']}({o['color']})" for o in opts) + "]"
        elif t == "multi_select":
            opts = info.get("multi_select", {}).get("options", [])
            extra = " options=[" + ", ".join(f"{o['name']}({o['color']})" for o in opts) + "]"
        marker = " [TITLE]" if t == "title" else ""
        print(f"  - {name!r:30s} type={t}{marker}{extra}")

    print("")
    # 첫 페이지 몇 개 미리보기
    try:
        pages = client.databases.query(database_id=db_id, page_size=3).get("results", [])
        print(f"Sample pages (first {len(pages)}):")
        for p in pages:
            title = ""
            for prop_val in p.get("properties", {}).values():
                if prop_val.get("type") == "title":
                    title = "".join(t.get("plain_text", "") for t in prop_val.get("title", []))
                    break
            print(f"  - {title!r}  (page_id={p['id']})")
    except Exception as e:
        print(f"  (page query failed: {e})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
