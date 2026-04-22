"""양방향 동기화 엔진.

안전 원칙: 데이터 삭제는 options의 명시적 플래그로만 활성화.
  - delete_gcal_when_notion_archived (default False): Notion archive 시 GCal 삭제 안 함
  - archive_notion_when_gcal_deleted (default False): GCal 삭제 시 Notion archive 안 함
  - create_notion_from_gcal (default False): GCal 쪽 orphan event를 Notion에 역방향 생성 안 함

알고리즘 (기본):
  1. Notion page들, GCal event들을 각자 전부 가져옴
  2. notion_page_id로 쌍을 만듦
  3. Notion → GCal 방향만 우선:
       - Notion에만 있음 → GCal 생성, Notion에 gcal_event_id 기록
       - 양쪽 존재 → conflict_resolution에 따라 승자 쪽을 반대편에 복제
       - GCal에만 있음은 기본적으로 건드리지 않음
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from config import Config, Mapping
from gcal_api import GCalAPI, GCalEvent
from notion_api import NotionAPI, NotionEvent


@dataclass
class SyncStats:
    notion_to_gcal_created: int = 0
    notion_to_gcal_updated: int = 0
    gcal_to_notion_created: int = 0
    gcal_to_notion_updated: int = 0
    gcal_deleted: int = 0
    notion_archived: int = 0
    skipped_safety: int = 0
    errors: int = 0


class SyncEngine:
    def __init__(self, config: Config, notion: NotionAPI, gcal: GCalAPI):
        self.config = config
        self.notion = notion
        self.gcal = gcal
        self.log = logging.getLogger("hansolcal.sync")

    def run(self) -> dict[str, SyncStats]:
        results: dict[str, SyncStats] = {}
        for mapping in self.config.mappings:
            self.log.info(f"=== Syncing mapping: {mapping.name} ===")
            results[mapping.name] = self._sync_one(mapping)
        return results

    # ────────────────────────────────────────
    def _sync_one(self, mapping: Mapping) -> SyncStats:
        stats = SyncStats()
        opts = self.config.options
        dry = opts.dry_run
        now = datetime.now(timezone.utc)
        time_min = now - timedelta(days=opts.past_days)
        time_max = now + timedelta(days=opts.future_days)

        notion_events = self.notion.list_events(mapping)
        gcal_events = self.gcal.list_events(mapping.google_calendar_id, time_min, time_max)
        self.log.info(f"  fetched: notion={len(notion_events)} gcal={len(gcal_events)}")

        # 인덱스
        notion_by_id: dict[str, NotionEvent] = {e.page_id: e for e in notion_events}
        gcal_by_notion_id: dict[str, GCalEvent] = {}
        gcal_by_gcal_id: dict[str, GCalEvent] = {e.event_id: e for e in gcal_events}
        orphan_gcal: list[GCalEvent] = []
        for ev in gcal_events:
            if ev.notion_page_id:
                gcal_by_notion_id[ev.notion_page_id] = ev
            else:
                orphan_gcal.append(ev)

        # 1) Notion 기준 루프
        for ne in notion_events:
            try:
                ge = gcal_by_notion_id.get(ne.page_id)
                # Notion에 기록된 gcal_event_id가 있고 인덱스엔 없으면 개별 조회 (시간창 밖일 수 있음)
                if ge is None and ne.gcal_event_id:
                    ge = self.gcal.get_event(mapping.google_calendar_id, ne.gcal_event_id)
                    if ge is not None:
                        gcal_by_notion_id[ne.page_id] = ge

                if ge is None:
                    # Notion에만 있음 → GCal 생성
                    if dry:
                        self.log.info(f"  [DRY] create GCal ← Notion '{ne.title}' color={ne.gcal_color_id}")
                    else:
                        new_ev = self.gcal.create_event(
                            calendar_id=mapping.google_calendar_id,
                            title=mapping.event_title_prefix + ne.title,
                            start=ne.start, end=ne.end, all_day=ne.all_day,
                            description=ne.description, location=ne.location,
                            notion_page_id=ne.page_id, color_id=ne.gcal_color_id,
                        )
                        self.notion.set_gcal_ref(ne.page_id, mapping.properties, new_ev.event_id, datetime.now(timezone.utc))
                        self.log.info(f"  ✓ GCal created: '{ne.title}' ({new_ev.event_id})")
                    stats.notion_to_gcal_created += 1
                else:
                    winner = _decide_winner(ne, ge, opts.conflict_resolution)
                    if winner == "notion":
                        if _needs_update_from_notion(ne, ge, mapping):
                            if dry:
                                self.log.info(f"  [DRY] update GCal ← Notion '{ne.title}'")
                            else:
                                self.gcal.update_event(
                                    calendar_id=mapping.google_calendar_id,
                                    event_id=ge.event_id,
                                    title=mapping.event_title_prefix + ne.title,
                                    start=ne.start, end=ne.end, all_day=ne.all_day,
                                    description=ne.description, location=ne.location,
                                    notion_page_id=ne.page_id, color_id=ne.gcal_color_id,
                                )
                                self.notion.set_gcal_ref(ne.page_id, mapping.properties, ge.event_id, datetime.now(timezone.utc))
                                self.log.info(f"  ✓ GCal updated: '{ne.title}'")
                            stats.notion_to_gcal_updated += 1
                    elif winner == "gcal":
                        if _needs_update_from_gcal(ne, ge, mapping):
                            if dry:
                                self.log.info(f"  [DRY] update Notion ← GCal '{ge.title}'")
                            else:
                                self.notion.update_page_from_gcal(
                                    page_id=ne.page_id,
                                    props=mapping.properties,
                                    title=_strip_prefix(ge.title, mapping.event_title_prefix),
                                    start=ge.start, end=ge.end, all_day=ge.all_day,
                                    description=ge.description, location=ge.location,
                                    synced_at=datetime.now(timezone.utc),
                                )
                                self.log.info(f"  ✓ Notion updated: '{ge.title}'")
                            stats.gcal_to_notion_updated += 1
                    gcal_by_gcal_id.pop(ge.event_id, None)

            except Exception as e:
                self.log.error(f"  ✗ error syncing Notion page {ne.page_id}: {e}", exc_info=True)
                stats.errors += 1

        # 2) Orphan GCal events — 안전 기본값: 건드리지 않음
        if orphan_gcal and not opts.create_notion_from_gcal:
            self.log.info(f"  (skipping {len(orphan_gcal)} orphan GCal event(s); set create_notion_from_gcal:true to import)")
            stats.skipped_safety += len(orphan_gcal)
        elif orphan_gcal:
            for ge in orphan_gcal:
                try:
                    if dry:
                        self.log.info(f"  [DRY] create Notion ← GCal '{ge.title}'")
                    else:
                        page_id = self.notion.create_page(
                            mapping,
                            title=ge.title,
                            start=ge.start, end=ge.end, all_day=ge.all_day,
                            description=ge.description, location=ge.location,
                            gcal_event_id=ge.event_id,
                            synced_at=datetime.now(timezone.utc),
                        )
                        self.gcal.update_event(
                            calendar_id=mapping.google_calendar_id,
                            event_id=ge.event_id,
                            title=ge.title,
                            start=ge.start, end=ge.end, all_day=ge.all_day,
                            description=ge.description, location=ge.location,
                            notion_page_id=page_id, color_id=ge.color_id,
                        )
                        self.log.info(f"  ✓ Notion created: '{ge.title}' ← GCal ({page_id})")
                    stats.gcal_to_notion_created += 1
                    gcal_by_gcal_id.pop(ge.event_id, None)
                except Exception as e:
                    self.log.error(f"  ✗ error creating Notion from GCal {ge.event_id}: {e}", exc_info=True)
                    stats.errors += 1

        # 3) 남은 GCal events (notion_page_id 있지만 Notion 쪽 page 없음) — 안전 기본값: 삭제 안 함
        dangling = [
            ev for ev in gcal_by_gcal_id.values()
            if ev.notion_page_id and ev.notion_page_id not in notion_by_id
        ]
        if dangling and not opts.delete_gcal_when_notion_archived:
            self.log.info(f"  (skipping {len(dangling)} dangling GCal event(s); set delete_gcal_when_notion_archived:true to clean up)")
            stats.skipped_safety += len(dangling)
        elif dangling:
            for ev in dangling:
                try:
                    if dry:
                        self.log.info(f"  [DRY] delete GCal '{ev.title}' (Notion page {ev.notion_page_id} gone)")
                    else:
                        self.gcal.delete_event(mapping.google_calendar_id, ev.event_id)
                        self.log.info(f"  ✓ GCal deleted: '{ev.title}'")
                    stats.gcal_deleted += 1
                except Exception as e:
                    self.log.error(f"  ✗ error deleting GCal {ev.event_id}: {e}", exc_info=True)
                    stats.errors += 1

        self.log.info(
            f"  stats: n→g create={stats.notion_to_gcal_created} update={stats.notion_to_gcal_updated} "
            f"g→n create={stats.gcal_to_notion_created} update={stats.gcal_to_notion_updated} "
            f"deleted={stats.gcal_deleted} skipped_safety={stats.skipped_safety} errors={stats.errors}"
        )
        return stats


# ────────────────────────────────────────
def _decide_winner(ne: NotionEvent, ge: GCalEvent, strategy: str) -> str:
    if strategy == "notion_wins":
        return "notion"
    if strategy == "gcal_wins":
        return "gcal"
    n_t = _as_utc(ne.last_edited_time)
    g_t = _as_utc(ge.updated)
    if n_t > g_t:
        return "notion"
    if g_t > n_t:
        return "gcal"
    return "tie"


def _as_utc(d: datetime) -> datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def _strip_prefix(title: str, prefix: str) -> str:
    if prefix and title.startswith(prefix):
        return title[len(prefix):]
    return title


def _needs_update_from_notion(ne: NotionEvent, ge: GCalEvent, mapping: Mapping) -> bool:
    expected_title = mapping.event_title_prefix + ne.title
    if expected_title != ge.title:
        return True
    if ne.all_day != ge.all_day:
        return True
    if _as_utc(ne.start) != _as_utc(ge.start):
        return True
    n_end = _as_utc(ne.end) if ne.end else None
    g_end = _as_utc(ge.end) if ge.end else None
    if n_end != g_end:
        return True
    if (ne.description or "") != (ge.description or ""):
        return True
    if (ne.location or "") != (ge.location or ""):
        return True
    if ne.gcal_color_id and ne.gcal_color_id != ge.color_id:
        return True
    return False


def _needs_update_from_gcal(ne: NotionEvent, ge: GCalEvent, mapping: Mapping) -> bool:
    g_title = _strip_prefix(ge.title, mapping.event_title_prefix)
    if g_title != ne.title:
        return True
    if ne.all_day != ge.all_day:
        return True
    if _as_utc(ne.start) != _as_utc(ge.start):
        return True
    n_end = _as_utc(ne.end) if ne.end else None
    g_end = _as_utc(ge.end) if ge.end else None
    if n_end != g_end:
        return True
    if (ne.description or "") != (ge.description or ""):
        return True
    if (ne.location or "") != (ge.location or ""):
        return True
    return False
