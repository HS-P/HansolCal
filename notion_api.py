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

from config import ColorMapping, Mapping, PropertyNames


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
