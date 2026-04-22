"""Notion API wrapper — 최소 기능 집합.

한 DB ↔ 한 Calendar 매핑용. property 이름은 config에서 주입.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from dateutil import parser as dtparser
from notion_client import Client

from config import ColorMapping, Mapping, PropertyNames, WeeklyPropertyNames


@dataclass
class WeeklyNotionEvent:
    """주간 반복 이벤트 (Notion 주간 일정 관리 DB)."""
    page_id: str
    title: str
    weekdays: list[str]          # ["MO", "WE", "FR"] (GCal BYDAY 형식)
    start_time: str              # "10:00"
    end_time: str                # "11:30"
    active: bool
    end_date: datetime | None    # 이 날까지만 반복
    gcal_event_id: str
    last_edited_time: datetime
    gcal_color_id: int = 0
    status_name: str = ""        # 상태 값 이름 (업무 DB row 생성 시 그대로 복사)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class NotionEvent:
    """정규화된 Notion 이벤트. page.id, last_edited_time, 필드 추출까지."""
    page_id: str
    title: str
    start: datetime
    end: datetime | None
    all_day: bool
    description: str
    location: str
    gcal_event_id: str
    last_edited_time: datetime
    # 색상 매핑 결과 (0 = 설정 안 함, 1~11 = GCal colorId)
    gcal_color_id: int = 0
    raw: dict[str, Any] = field(default_factory=dict)


class NotionAPI:
    def __init__(self, token: str | None = None):
        self.client = Client(auth=token or os.environ["NOTION_TOKEN"])

    # ────────────────────────────────────────
    # Read
    # ────────────────────────────────────────
    def list_events(self, mapping: Mapping) -> list[NotionEvent]:
        """DB의 모든 page를 NotionEvent로 변환. archived 제외."""
        events: list[NotionEvent] = []
        cursor: str | None = None
        while True:
            resp = self.client.databases.query(
                database_id=mapping.notion_database_id,
                start_cursor=cursor,
                page_size=100,
            )
            for page in resp.get("results", []):
                if page.get("archived"):
                    continue
                ev = self._parse_page(page, mapping.properties, mapping.color_mapping)
                if ev is not None:
                    events.append(ev)
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return events

    def _parse_page(self, page: dict, props: PropertyNames, color_map: ColorMapping) -> NotionEvent | None:
        p = page.get("properties", {})

        # 타이틀은 config에 있으면 그것 우선, 없으면 type=title 자동 탐지
        title = _extract_title(p, props.title)

        date_prop = p.get(props.date) or {}
        date_val = date_prop.get("date")
        if not date_val or not date_val.get("start"):
            return None  # date 없으면 skip

        start_raw = date_val["start"]
        end_raw = date_val.get("end")
        start, all_day_start = _parse_date(start_raw)
        end, _ = _parse_date(end_raw) if end_raw else (None, False)

        description = _rich_text(p.get(props.description)) if props.description else ""
        location = _rich_text(p.get(props.location)) if props.location else ""
        gcal_id = _rich_text(p.get(props.gcal_event_id))

        last_edited = dtparser.isoparse(page["last_edited_time"])

        # 색상 매핑
        gcal_color_id = 0
        if color_map.source_property:
            gcal_color_id = _resolve_color_id(p.get(color_map.source_property), color_map)

        return NotionEvent(
            page_id=page["id"],
            title=title,
            start=start,
            end=end,
            all_day=all_day_start,
            description=description,
            location=location,
            gcal_event_id=gcal_id,
            last_edited_time=last_edited,
            gcal_color_id=gcal_color_id,
            raw=page,
        )

    # ────────────────────────────────────────
    # Write
    # ────────────────────────────────────────
    def set_gcal_ref(self, page_id: str, props: PropertyNames, gcal_event_id: str, synced_at: datetime) -> None:
        self.client.pages.update(
            page_id=page_id,
            properties={
                props.gcal_event_id: _text_prop(gcal_event_id),
                props.gcal_updated: _text_prop(synced_at.isoformat()),
            },
        )

    def create_page(self, mapping: Mapping, *, title: str, start: datetime, end: datetime | None,
                    all_day: bool, description: str, location: str, gcal_event_id: str,
                    synced_at: datetime) -> str:
        props = mapping.properties
        # 타이틀 property 이름을 DB 스키마에서 탐지 (config에 잘못 써있어도 동작)
        title_prop_name = self._find_title_property_name(mapping.notion_database_id, props.title)

        properties: dict[str, Any] = {
            title_prop_name: {"title": [{"type": "text", "text": {"content": title or "(untitled)"}}]},
            props.date: {"date": _to_notion_date(start, end, all_day)},
            props.gcal_event_id: _text_prop(gcal_event_id),
            props.gcal_updated: _text_prop(synced_at.isoformat()),
        }
        if props.description and description:
            properties[props.description] = _text_prop(description)
        if props.location and location:
            properties[props.location] = _text_prop(location)

        page = self.client.pages.create(
            parent={"database_id": mapping.notion_database_id},
            properties=properties,
        )
        return page["id"]

    def update_page_from_gcal(self, page_id: str, props: PropertyNames, *,
                              title: str, start: datetime, end: datetime | None, all_day: bool,
                              description: str, location: str, synced_at: datetime) -> None:
        # page의 title property 이름 탐지
        page = self.client.pages.retrieve(page_id=page_id)
        title_prop_name = _find_title_in_page(page, props.title)

        properties: dict[str, Any] = {
            title_prop_name: {"title": [{"type": "text", "text": {"content": title or "(untitled)"}}]},
            props.date: {"date": _to_notion_date(start, end, all_day)},
            props.gcal_updated: _text_prop(synced_at.isoformat()),
        }
        if props.description:
            properties[props.description] = _text_prop(description)
        if props.location:
            properties[props.location] = _text_prop(location)
        self.client.pages.update(page_id=page_id, properties=properties)

    def archive_page(self, page_id: str) -> None:
        self.client.pages.update(page_id=page_id, archived=True)

    # ═══════════════════════════════════════════════════════════════
    # Weekly DB (주간 일정 관리)
    # ═══════════════════════════════════════════════════════════════
    def list_weekly_events(self, mapping: Mapping) -> list[WeeklyNotionEvent]:
        wp = mapping.weekly_properties
        cm = mapping.color_mapping
        out: list[WeeklyNotionEvent] = []
        cursor: str | None = None
        while True:
            resp = self.client.databases.query(
                database_id=mapping.notion_database_id,
                start_cursor=cursor, page_size=100,
            )
            for page in resp.get("results", []):
                if page.get("archived"):
                    continue
                ev = _parse_weekly_page(page, wp, cm)
                if ev is not None:
                    out.append(ev)
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return out

    def list_derived_rows(self, target_mapping: Mapping, source_property: str) -> list[dict]:
        """업무 DB에서 '주간출처' 값이 있는 row들의 (source, date) 목록 반환."""
        out: list[dict] = []
        cursor: str | None = None
        tp = target_mapping.properties
        while True:
            resp = self.client.databases.query(
                database_id=target_mapping.notion_database_id,
                filter={"property": source_property, "rich_text": {"is_not_empty": True}},
                start_cursor=cursor, page_size=100,
            )
            for page in resp.get("results", []):
                if page.get("archived"):
                    continue
                src = _rich_text(page.get("properties", {}).get(source_property))
                date_prop = page.get("properties", {}).get(tp.date) or {}
                date_val = date_prop.get("date") or {}
                start_raw = date_val.get("start", "")
                if not src or not start_raw:
                    continue
                # 날짜 부분만 뽑음 (YYYY-MM-DD)
                date_iso = start_raw[:10]
                out.append({"page_id": page["id"], "source": src.strip(), "date": date_iso})
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return out

    def create_task_from_weekly(self, target_mapping: Mapping, source_property: str,
                                source_weekly_id: str, title: str, instance_date,
                                start_time: str, end_time: str,
                                status_name: str, tz_name: str) -> str:
        """주간 템플릿 기반으로 업무 DB에 단일 row 생성."""
        import zoneinfo
        from datetime import time as dtime, datetime as _dt
        tz = zoneinfo.ZoneInfo(tz_name)

        sh, sm = [int(x) for x in start_time.split(":")]
        eh, em = [int(x) for x in end_time.split(":")]
        start_dt = _dt.combine(instance_date, dtime(sh, sm), tzinfo=tz)
        end_dt = _dt.combine(instance_date, dtime(eh, em), tzinfo=tz)
        if end_dt <= start_dt:
            from datetime import timedelta as _td
            end_dt = start_dt + _td(hours=1)

        tp = target_mapping.properties
        title_prop_name = self._find_title_property_name(
            target_mapping.notion_database_id, tp.title
        )

        properties: dict[str, Any] = {
            title_prop_name: {"title": [{"type": "text", "text": {"content": title or "(untitled)"}}]},
            tp.date: {"date": {"start": start_dt.isoformat(), "end": end_dt.isoformat()}},
            source_property: _text_prop(source_weekly_id),
        }
        if status_name:
            properties["상태"] = {"status": {"name": status_name}}

        page = self.client.pages.create(
            parent={"database_id": target_mapping.notion_database_id},
            properties=properties,
        )
        return page["id"]

    def set_gcal_ref_weekly(self, page_id: str, wp: WeeklyPropertyNames,
                            gcal_event_id: str, synced_at: datetime) -> None:
        self.client.pages.update(
            page_id=page_id,
            properties={
                wp.gcal_event_id: _text_prop(gcal_event_id),
                wp.gcal_updated: _text_prop(synced_at.isoformat()),
            },
        )

    # ────────────────────────────────────────
    def _find_title_property_name(self, database_id: str, fallback: str) -> str:
        """DB 스키마에서 type=title 인 property 이름 탐지 (캐시)."""
        if not hasattr(self, "_title_cache"):
            self._title_cache: dict[str, str] = {}
        if database_id in self._title_cache:
            return self._title_cache[database_id]
        try:
            db = self.client.databases.retrieve(database_id=database_id)
            for name, info in db.get("properties", {}).items():
                if info.get("type") == "title":
                    self._title_cache[database_id] = name
                    return name
        except Exception:
            pass
        return fallback


# ────────────────────────────────────────
# Helpers
# ────────────────────────────────────────
def _extract_title(props: dict, configured_name: str) -> str:
    # 우선 configured 이름 체크, 없으면 type=title 전체 스캔
    prop = props.get(configured_name)
    if prop and prop.get("type") == "title":
        return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    for prop_val in props.values():
        if prop_val.get("type") == "title":
            return "".join(t.get("plain_text", "") for t in prop_val.get("title", []))
    return ""


def _find_title_in_page(page: dict, fallback: str) -> str:
    for name, info in page.get("properties", {}).items():
        if info.get("type") == "title":
            return name
    return fallback


def _rich_text(prop: dict | None) -> str:
    if not prop:
        return ""
    arr = prop.get("rich_text")
    if isinstance(arr, list):
        return "".join(item.get("plain_text", "") for item in arr)
    arr = prop.get("title")
    if isinstance(arr, list):
        return "".join(item.get("plain_text", "") for item in arr)
    return ""


def _text_prop(text: str) -> dict:
    return {"rich_text": [{"type": "text", "text": {"content": text or ""}}]}


def _parse_date(raw: str | None) -> tuple[datetime | None, bool]:
    if raw is None:
        return None, False
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        return dtparser.isoparse(raw), True
    return dtparser.isoparse(raw), False


def _to_notion_date(start: datetime, end: datetime | None, all_day: bool) -> dict:
    def _fmt(d: datetime) -> str:
        if all_day:
            return d.strftime("%Y-%m-%d")
        return d.isoformat()
    out: dict[str, Any] = {"start": _fmt(start)}
    if end is not None:
        out["end"] = _fmt(end)
    return out


# Notion 한글 요일 → GCal BYDAY 코드
_WEEKDAY_MAP = {
    "월": "MO", "화": "TU", "수": "WE", "목": "TH",
    "금": "FR", "토": "SA", "일": "SU",
}


def _parse_weekly_page(page: dict, wp: WeeklyPropertyNames, cm: ColorMapping) -> WeeklyNotionEvent | None:
    p = page.get("properties", {})
    title = _extract_title(p, wp.title)

    # 요일 multi-select
    wd_prop = p.get(wp.weekday, {}) or {}
    wd_options = wd_prop.get("multi_select", []) or []
    weekdays: list[str] = []
    for opt in wd_options:
        name = (opt.get("name") or "").strip()
        code = _WEEKDAY_MAP.get(name)
        if code:
            weekdays.append(code)
    if not weekdays:
        return None  # 요일 미선택 → skip

    start_time = _rich_text(p.get(wp.start_time)).strip()
    end_time = _rich_text(p.get(wp.end_time)).strip()
    if not start_time or not end_time:
        return None

    active_prop = p.get(wp.active) or {}
    active = bool(active_prop.get("checkbox", False))

    end_date: datetime | None = None
    if wp.end_date:
        ed_prop = p.get(wp.end_date) or {}
        ed_val = ed_prop.get("date")
        if ed_val and ed_val.get("start"):
            end_date, _ = _parse_date(ed_val["start"])

    gcal_id = _rich_text(p.get(wp.gcal_event_id))
    last_edited = dtparser.isoparse(page["last_edited_time"])

    gcal_color_id = 0
    status_name = ""
    if cm.source_property:
        gcal_color_id = _resolve_color_id(p.get(cm.source_property), cm)
        st_prop = p.get(cm.source_property) or {}
        t = st_prop.get("type")
        if t == "status":
            status_name = (st_prop.get("status") or {}).get("name", "") or ""
        elif t == "select":
            status_name = (st_prop.get("select") or {}).get("name", "") or ""

    return WeeklyNotionEvent(
        page_id=page["id"],
        title=title,
        weekdays=weekdays,
        start_time=start_time,
        end_time=end_time,
        active=active,
        end_date=end_date,
        gcal_event_id=gcal_id,
        last_edited_time=last_edited,
        gcal_color_id=gcal_color_id,
        status_name=status_name,
        raw=page,
    )


def _resolve_color_id(prop: dict | None, cm: ColorMapping) -> int:
    """Status / Select / Multi-select property에서 값과 색을 추출해 GCal colorId로 매핑.

    우선순위:
      1) cm.by_name[값 이름]
      2) cm.by_notion_color[Notion 색 이름]
      3) cm.default_color_id
    """
    if not prop:
        return cm.default_color_id

    t = prop.get("type")
    name: str = ""
    color: str = "default"

    if t == "status":
        st = prop.get("status") or {}
        name = st.get("name", "") or ""
        color = st.get("color", "default") or "default"
    elif t == "select":
        sel = prop.get("select") or {}
        name = sel.get("name", "") or ""
        color = sel.get("color", "default") or "default"
    elif t == "multi_select":
        arr = prop.get("multi_select") or []
        if arr:
            name = arr[0].get("name", "") or ""
            color = arr[0].get("color", "default") or "default"
    else:
        return cm.default_color_id

    if name and name in cm.by_name:
        return cm.by_name[name]
    if color in cm.by_notion_color:
        return cm.by_notion_color[color]
    return cm.default_color_id
