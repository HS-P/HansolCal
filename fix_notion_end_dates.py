"""Notion page의 end date 원상 복구.

조건: date property가 all-day이고 end - start == 1일 (정확히 1일)인 페이지
→ end 필드 제거 (당일치기로 복구)

사용:
  python fix_notion_end_dates.py --dry-run    # 대상 목록만
  python fix_notion_end_dates.py              # 실제 복구
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta
from pathlib import Path

from notion_client import Client


def _load_env(path: str = ".env") -> None:
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--database-id", default="2b5fa7b54ead8113ae49c6d1efbfbdc6")
    parser.add_argument("--date-property", default="날짜")
    args = parser.parse_args()

    _load_env()
    client = Client(auth=os.environ["NOTION_TOKEN"])

    # 1) 모든 페이지 수집
    pages = []
    cursor = None
    while True:
        resp = client.databases.query(
            database_id=args.database_id, start_cursor=cursor, page_size=100
        )
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    print(f"Scanning {len(pages)} pages...")

    # 2) 복구 대상 식별
    candidates = []
    for page in pages:
        if page.get("archived"):
            continue
        date_prop = page.get("properties", {}).get(args.date_property)
        if not date_prop:
            continue
        date_val = date_prop.get("date")
        if not date_val:
            continue
        start_raw = date_val.get("start")
        end_raw = date_val.get("end")
        if not start_raw or not end_raw:
            continue
        # all-day 판정: length 10 (YYYY-MM-DD)
        if len(start_raw) != 10 or len(end_raw) != 10:
            continue
        # start + 1일 == end ?
        from datetime import date
        s = date.fromisoformat(start_raw)
        e = date.fromisoformat(end_raw)
        if (e - s) != timedelta(days=1):
            continue

        # 제목 뽑기
        title = ""
        for prop_val in page.get("properties", {}).values():
            if prop_val.get("type") == "title":
                title = "".join(t.get("plain_text", "") for t in prop_val.get("title", []))
                break
        candidates.append((page["id"], title, start_raw, end_raw))

    print(f"Candidates (all-day, end = start+1d): {len(candidates)}")
    print("")
    print("Sample (first 20):")
    for pid, title, s, e in candidates[:20]:
        print(f"  {title!r:30s}  {s} → {e}  (복구: end 제거)")
    if len(candidates) > 20:
        print(f"  ... and {len(candidates) - 20} more")

    if args.dry_run:
        print("")
        print("[DRY RUN] 실제 수정 없음. 확인 후 --dry-run 빼고 재실행하세요.")
        return 0

    # 3) 실제 복구
    print("")
    print(f"Updating {len(candidates)} pages...")
    import time
    ok, err = 0, 0
    for i, (pid, title, s, e) in enumerate(candidates, 1):
        try:
            client.pages.update(
                page_id=pid,
                properties={
                    args.date_property: {"date": {"start": s}},  # end 제외 → null
                },
            )
            ok += 1
            if i % 20 == 0 or i == len(candidates):
                print(f"  [{i}/{len(candidates)}] OK")
        except Exception as ex:
            err += 1
            print(f"  ✗ {title!r}: {ex}")
        time.sleep(0.35)  # Notion rate limit ~3/s

    print(f"\nDone: ok={ok} err={err}")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
