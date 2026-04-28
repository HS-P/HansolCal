"""Sync engine for Notion-backed calendar data.

Policy:
- Notion is the source of truth.
- Weekly templates create task rows for the configured past/future window.
- Google Calendar events are created/updated from Notion rows.
- Google Calendar events without a Notion mapping are deleted from managed calendars.
- Notion rows are never archived or overwritten from Google Calendar.
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
        weekly_mappings = [m for m in self.config.mappings if m.kind == "weekly"]
        event_mappings = [m for m in self.config.mappings if m.kind != "weekly"]

        # Build missing Notion task rows before syncing those rows to Google Calendar.
        for mapping in weekly_mappings + event_mappings:
            self.log.info(f"=== Syncing mapping: {mapping.name} (kind={mapping.kind}) ===")
            if mapping.kind == "weekly":
                results[mapping.name] = self._sync_weekly(mapping)
            else:
                results[mapping.name] = self._sync_one(mapping)
        return results

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

        notion_by_id: dict[str, NotionEvent] = {e.page_id: e for e in notion_events}
        gcal_by_notion_id: dict[str, GCalEvent] = {}
        gcal_by_gcal_id: dict[str, GCalEvent] = {e.event_id: e for e in gcal_events}
        orphan_gcal: list[GCalEvent] = []
        matched_gcal_ids: set[str] = set()

        for ev in gcal_events:
            if ev.notion_page_id:
                gcal_by_notion_id[ev.notion_page_id] = ev
            elif not _can_delete_gcal_event(ev):
                stats.skipped_safety += 1
                self.log.info(
                    f"  skip unmanaged GCal '{ev.title}' ({ev.event_id}); "
                    f"event_type={ev.event_type}"
                )
            else:
                orphan_gcal.append(ev)

        for ne in notion_events:
            try:
                ge = gcal_by_notion_id.get(ne.page_id)
                if ge is None and ne.gcal_event_id:
                    ge = self.gcal.get_event(mapping.google_calendar_id, ne.gcal_event_id)
                    if ge is not None:
                        gcal_by_notion_id[ne.page_id] = ge

                if ge is None:
                    if dry:
                        self.log.info(f"  [DRY] create GCal <- Notion '{ne.title}' color={ne.gcal_color_id}")
                    else:
                        new_ev = self.gcal.create_event(
                            calendar_id=mapping.google_calendar_id,
                            title=mapping.event_title_prefix + ne.title,
                            start=ne.start,
                            end=ne.end,
                            all_day=ne.all_day,
                            description=ne.description,
                            location=ne.location,
                            notion_page_id=ne.page_id,
                            color_id=ne.gcal_color_id,
                        )
                        self.notion.set_gcal_ref(
                            ne.page_id,
                            mapping.properties,
                            new_ev.event_id,
                            datetime.now(timezone.utc),
                        )
                        matched_gcal_ids.add(new_ev.event_id)
                        self.log.info(f"  OK GCal created: '{ne.title}' ({new_ev.event_id})")
                    stats.notion_to_gcal_created += 1
                    continue

                matched_gcal_ids.add(ge.event_id)
                if _needs_update_from_notion(ne, ge, mapping):
                    if dry:
                        self.log.info(f"  [DRY] update GCal <- Notion '{ne.title}'")
                    else:
                        self.gcal.update_event(
                            calendar_id=mapping.google_calendar_id,
                            event_id=ge.event_id,
                            title=mapping.event_title_prefix + ne.title,
                            start=ne.start,
                            end=ne.end,
                            all_day=ne.all_day,
                            description=ne.description,
                            location=ne.location,
                            notion_page_id=ne.page_id,
                            color_id=ne.gcal_color_id,
                        )
                        self.notion.set_gcal_ref(
                            ne.page_id,
                            mapping.properties,
                            ge.event_id,
                            datetime.now(timezone.utc),
                        )
                        self.log.info(f"  OK GCal updated: '{ne.title}'")
                    stats.notion_to_gcal_updated += 1
                gcal_by_gcal_id.pop(ge.event_id, None)

            except Exception as e:
                self.log.error(f"  error syncing Notion page {ne.page_id}: {e}", exc_info=True)
                stats.errors += 1

        # This repo's managed calendars are Notion mirrors. Delete Google-only events.
        unmatched_orphans = [ev for ev in orphan_gcal if ev.event_id not in matched_gcal_ids]
        for ge in unmatched_orphans:
            try:
                if dry:
                    self.log.info(f"  [DRY] delete orphan GCal '{ge.title}' ({ge.event_id})")
                else:
                    self.gcal.delete_event(mapping.google_calendar_id, ge.event_id)
                    self.log.info(f"  OK orphan GCal deleted: '{ge.title}' ({ge.event_id})")
                stats.gcal_deleted += 1
                gcal_by_gcal_id.pop(ge.event_id, None)
            except Exception as e:
                self.log.error(f"  error deleting orphan GCal {ge.event_id}: {e}", exc_info=True)
                stats.errors += 1

        dangling = [
            ev for ev in gcal_by_gcal_id.values()
            if ev.notion_page_id and ev.notion_page_id not in notion_by_id
        ]
        for ev in dangling:
            try:
                if not _can_delete_gcal_event(ev):
                    stats.skipped_safety += 1
                    self.log.info(
                        f"  skip dangling GCal '{ev.title}' ({ev.event_id}); "
                        f"event_type={ev.event_type}"
                    )
                    continue
                if dry:
                    self.log.info(f"  [DRY] delete dangling GCal '{ev.title}' (Notion page {ev.notion_page_id} missing)")
                else:
                    self.gcal.delete_event(mapping.google_calendar_id, ev.event_id)
                    self.log.info(f"  OK dangling GCal deleted: '{ev.title}'")
                stats.gcal_deleted += 1
            except Exception as e:
                self.log.error(f"  error deleting dangling GCal {ev.event_id}: {e}", exc_info=True)
                stats.errors += 1

        self.log.info(
            f"  stats: n->g create={stats.notion_to_gcal_created} update={stats.notion_to_gcal_updated} "
            f"g->n create={stats.gcal_to_notion_created} update={stats.gcal_to_notion_updated} "
            f"deleted={stats.gcal_deleted} skipped_safety={stats.skipped_safety} errors={stats.errors}"
        )
        return stats

    def _sync_weekly(self, mapping: Mapping) -> SyncStats:
        stats = SyncStats()
        dry = self.config.options.dry_run

        target = _find_target_mapping(self.config, mapping)
        if target is None:
            self.log.error(f"  target events mapping not found for weekly '{mapping.name}'")
            stats.errors += 1
            return stats

        items = self.notion.list_weekly_events(mapping)
        self.log.info(f"  fetched weekly templates: {len(items)}")

        import zoneinfo
        tz = zoneinfo.ZoneInfo(mapping.timezone)
        today = datetime.now(tz).date()
        start_date = today - timedelta(days=self.config.options.past_days)
        horizon = today + timedelta(weeks=mapping.weekly_lookahead_weeks)

        derived = self.notion.list_derived_rows(target, mapping.target_source_property)
        existing_set: set[tuple[str, str]] = set()
        for d in derived:
            existing_set.add((d["source"], d["date"]))

        weekday_to_index = {"MO": 0, "TU": 1, "WE": 2, "TH": 3, "FR": 4, "SA": 5, "SU": 6}

        for w in items:
            try:
                if not w.active:
                    continue
                if w.end_date and w.end_date.date() < start_date:
                    continue

                wd_indices = sorted(weekday_to_index[wd] for wd in w.weekdays)
                for single_day in _iter_days(start_date, horizon):
                    if single_day.weekday() not in wd_indices:
                        continue
                    if w.end_date and single_day > w.end_date.date():
                        continue

                    key = (w.page_id, single_day.isoformat())
                    if key in existing_set:
                        continue

                    if dry:
                        self.log.info(
                            f"  [DRY] create task row: '{w.title}' {single_day} "
                            f"{w.start_time}-{w.end_time} [{w.status_name}]"
                        )
                    else:
                        new_page_id = self.notion.create_task_from_weekly(
                            target_mapping=target,
                            source_property=mapping.target_source_property,
                            source_weekly_id=w.page_id,
                            title=w.title,
                            instance_date=single_day,
                            start_time=w.start_time,
                            end_time=w.end_time,
                            status_name=w.status_name,
                            tz_name=mapping.timezone,
                        )
                        self.log.info(
                            f"  OK task row created: '{w.title}' {single_day} (page={new_page_id[:8]})"
                        )
                    existing_set.add(key)
                    stats.notion_to_gcal_created += 1
            except Exception as e:
                self.log.error(f"  weekly template error '{w.title}': {e}", exc_info=True)
                stats.errors += 1

        self.log.info(
            f"  weekly stats: rows_created={stats.notion_to_gcal_created} errors={stats.errors}"
        )
        return stats


def _find_target_mapping(config: Config, weekly_mapping: Mapping) -> Mapping | None:
    name = weekly_mapping.target_events_mapping.strip()
    if name:
        for m in config.mappings:
            if m.name == name and m.kind == "events":
                return m
    for m in config.mappings:
        if m.kind == "events":
            return m
    return None


def _iter_days(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d = d + timedelta(days=1)


def _as_utc(d: datetime) -> datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def _can_delete_gcal_event(ev: GCalEvent) -> bool:
    return ev.event_type == "default" and not ev.raw.get("locked", False)


def _needs_update_from_notion(ne: NotionEvent, ge: GCalEvent, mapping: Mapping) -> bool:
    expected_title = mapping.event_title_prefix + ne.title
    if ge.notion_page_id != ne.page_id:
        return True
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
