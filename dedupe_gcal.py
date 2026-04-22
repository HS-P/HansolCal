"""GCal 중복 이벤트 정리.

알고리즘:
  1. (title, start.isoformat, all_day) 기준으로 그룹핑
  2. 크기 > 1 그룹마다:
       - canonical 선정 우선순위:
           (a) notion_page_id 없는 orphan 중 가장 먼저 생성된 것 (사용자 원본)
           (b) 없으면 우리가 만든 이벤트 중 하나 (Notion의 gcal_event_id가 가리키는 것 우선)
       - canonical에 Notion과의 연결을 보장:
           - orphan이 canonical이면 → Notion의 gcal_event_id를 canonical.event_id로 교체
           - orphan 쪽 extendedProperties에 notion_page_id 주입
       - 나머지 중복 이벤트 삭제 (GCal만; Notion page는 건드리지 않음)

사용:
  python dedupe_gcal.py --dry-run
  python dedupe_gcal.py
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


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
    args = parser.parse_args()

    _load_env()

    from config import load_config
    from notion_api import NotionAPI
    from gcal_api import GCalAPI

    cfg = load_config("config.yaml")
    m = cfg.mappings[0]
    notion = NotionAPI()
    gcal = GCalAPI()

    # 넉넉한 윈도우 (과거 1년 ~ 미래 2년)
    now = datetime.now(timezone.utc)
    events = gcal.list_events(m.google_calendar_id, now - timedelta(days=365), now + timedelta(days=730))
    print(f"Fetched {len(events)} GCal events")

    # Notion 페이지 ID → 현재 Notion에 저장된 gcal_event_id
    ne_list = notion.list_events(m)
    notion_gcal_ref: dict[str, str] = {ne.page_id: ne.gcal_event_id for ne in ne_list}
    notion_title_by_id: dict[str, str] = {ne.page_id: ne.title for ne in ne_list}

    # 그룹핑
    groups: dict[tuple, list] = defaultdict(list)
    for e in events:
        key = (e.title, e.start.isoformat() if e.start else "", e.all_day)
        groups[key].append(e)
    dups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"Duplicate groups: {len(dups)} (total events in groups: {sum(len(v) for v in dups.values())})")

    if not dups:
        print("No duplicates. Exiting.")
        return 0

    print("")
    print("Plan:")
    # action 리스트: (type, ...)
    # type: "set_notion_ref"(page_id, new_gcal_id), "set_extprop"(event_id, notion_page_id), "delete"(event_id)
    actions = []

    for key, evs in sorted(dups.items()):
        title, start, all_day = key
        # canonical 선정
        orphans = [e for e in evs if not e.notion_page_id]
        ours = [e for e in evs if e.notion_page_id]

        if orphans:
            # 가장 먼저 생성된 orphan = canonical (사용자 원본)
            canonical = sorted(orphans, key=lambda e: e.updated)[0]
            # canonical이 연결된 Notion page 찾기 (우리 것 중 이 그룹에 매핑되어 있는 것)
            linked_notion_ids = [e.notion_page_id for e in ours if e.notion_page_id in notion_gcal_ref]
            target_notion_id = linked_notion_ids[0] if linked_notion_ids else None

            print(f"  [{title!r}] start={start}  size={len(evs)}")
            print(f"    canonical = {canonical.event_id} (orphan, updated={canonical.updated})")

            if target_notion_id:
                # orphan에 notion_page_id 주입 + Notion의 gcal_event_id를 canonical로 교체
                actions.append(("set_extprop", canonical.event_id, target_notion_id, canonical))
                actions.append(("set_notion_ref", target_notion_id, canonical.event_id))
                print(f"    → orphan에 notion_page_id={target_notion_id[:8]}… 주입")
                print(f"    → Notion page.gcal_event_id ← {canonical.event_id}")

            # 나머지 전부 삭제
            for e in evs:
                if e.event_id == canonical.event_id:
                    continue
                actions.append(("delete", e.event_id, e.title, e.start))
                marker = " (ours)" if e.notion_page_id else " (orphan-dup)"
                print(f"    → delete {e.event_id}{marker}")
        else:
            # orphan 없음, 우리 것만 여러 개 → Notion의 gcal_event_id가 가리키는 것 우선
            pointed = None
            for e in ours:
                npid = e.notion_page_id
                if npid in notion_gcal_ref and notion_gcal_ref[npid] == e.event_id:
                    pointed = e
                    break
            canonical = pointed or sorted(ours, key=lambda e: e.updated)[0]
            print(f"  [{title!r}] start={start}  size={len(evs)}  (no orphan)")
            print(f"    canonical = {canonical.event_id} (ours)")
            for e in evs:
                if e.event_id == canonical.event_id:
                    continue
                actions.append(("delete", e.event_id, e.title, e.start))
                print(f"    → delete {e.event_id}")

    print("")
    print(f"Total actions: {len(actions)}")
    if args.dry_run:
        print("[DRY RUN]")
        return 0

    import time
    counts = {"set_extprop": 0, "set_notion_ref": 0, "delete": 0, "error": 0}
    for act in actions:
        try:
            if act[0] == "set_extprop":
                _, event_id, notion_page_id, ev = act
                # GCal event update (extendedProperties.private.notion_page_id 세팅)
                end_arg = ev.end
                gcal.update_event(
                    calendar_id=m.google_calendar_id, event_id=event_id,
                    title=ev.title, start=ev.start, end=end_arg, all_day=ev.all_day,
                    description=ev.description, location=ev.location,
                    notion_page_id=notion_page_id, color_id=ev.color_id,
                )
                counts["set_extprop"] += 1
            elif act[0] == "set_notion_ref":
                _, page_id, new_gcal_id = act
                notion.set_gcal_ref(page_id, m.properties, new_gcal_id, datetime.now(timezone.utc))
                counts["set_notion_ref"] += 1
            elif act[0] == "delete":
                _, event_id, *_ = act
                gcal.delete_event(m.google_calendar_id, event_id)
                counts["delete"] += 1
        except Exception as e:
            counts["error"] += 1
            print(f"  ✗ action {act[0]} failed: {e}")
        time.sleep(0.3)

    print("")
    print(f"Done: {counts}")
    return 0 if counts["error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
