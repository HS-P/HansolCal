"""config.yaml 로더."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


# Notion 기본 색 → GCal colorId fallback 매핑
_DEFAULT_NOTION_COLOR_TO_GCAL: dict[str, int] = {
    "red": 11,       # Tomato
    "orange": 6,     # Tangerine
    "yellow": 5,     # Banana
    "green": 10,     # Basil
    "blue": 9,       # Blueberry
    "purple": 3,     # Grape
    "pink": 4,       # Flamingo
    "brown": 8,      # Graphite (GCal에 brown 없음)
    "gray": 8,       # Graphite
    "default": 0,    # 0 = unset (GCal 기본)
}


@dataclass
class PropertyNames:
    title: str = "Name"
    date: str = "Date"
    description: str | None = "Description"
    location: str | None = "Location"
    gcal_event_id: str = "gcal_event_id"
    gcal_updated: str = "gcal_updated"


@dataclass
class ColorMapping:
    """Notion property 값 → GCal colorId(1~11) 매핑.

    source_property 가 비어있으면 색상 매핑 비활성.
    """
    source_property: str = ""         # Notion의 Status / Select / Multi-select property 이름
    by_name: dict[str, int] = field(default_factory=dict)          # 값 이름 → colorId (우선)
    by_notion_color: dict[str, int] = field(default_factory=lambda: dict(_DEFAULT_NOTION_COLOR_TO_GCAL))
    default_color_id: int = 0         # 매핑 실패 시 (0 = 설정 안 함)


@dataclass
class WeeklyPropertyNames:
    """주간 반복 일정 DB용 property 이름."""
    title: str = "이름"
    weekday: str = "요일"             # multi-select (월 화 수 목 금 토 일)
    start_time: str = "시작 시간"      # rich text "HH:MM"
    end_time: str = "종료 시간"        # rich text "HH:MM"
    active: str = "활성"              # checkbox
    end_date: str | None = "종료일"    # date (optional)
    gcal_event_id: str = "gcal_event_id"
    gcal_updated: str = "gcal_updated"


@dataclass
class Mapping:
    name: str
    notion_database_id: str
    google_calendar_id: str
    properties: PropertyNames
    color_mapping: ColorMapping = field(default_factory=ColorMapping)
    event_title_prefix: str = ""
    # "events" = 일반 일정 DB (기존), "weekly" = 주간 반복 DB (업무 DB에 row 자동 생성)
    kind: str = "events"
    weekly_properties: WeeklyPropertyNames = field(default_factory=WeeklyPropertyNames)
    timezone: str = "Asia/Seoul"
    # weekly 전용: 앞으로 몇 주치 인스턴스를 업무 DB에 미리 생성할지
    weekly_lookahead_weeks: int = 2
    # weekly 전용: 인스턴스 row를 넣을 대상 events 매핑 이름
    target_events_mapping: str = ""
    # weekly 전용: 업무 DB의 "주간출처" property 이름 (파생 row 식별)
    target_source_property: str = "주간출처"


@dataclass
class Options:
    past_days: int = 30
    future_days: int = 365
    conflict_resolution: str = "last_write_wins"
    dry_run: bool = False
    log_level: str = "INFO"
    # 안전 플래그 (default False — 데이터 삭제 방지 우선)
    delete_gcal_when_notion_archived: bool = False
    archive_notion_when_gcal_deleted: bool = False
    # GCal에만 있는 이벤트를 Notion으로 가져올지 (default False — 일단 단방향만)
    create_notion_from_gcal: bool = False


@dataclass
class Config:
    mappings: list[Mapping] = field(default_factory=list)
    options: Options = field(default_factory=Options)


def load_config(path: str | Path = "config.yaml") -> Config:
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(
            f"{p} not found. Copy config.example.yaml to config.yaml and edit."
        )
    raw: dict[str, Any] = yaml.safe_load(p.read_text(encoding="utf-8"))

    mappings: list[Mapping] = []
    for m in raw.get("mappings", []):
        props_raw = m.get("properties", {}) or {}
        props = PropertyNames(
            title=props_raw.get("title", "Name"),
            date=props_raw.get("date", "Date"),
            description=props_raw.get("description") or None,
            location=props_raw.get("location") or None,
            gcal_event_id=props_raw.get("gcal_event_id", "gcal_event_id"),
            gcal_updated=props_raw.get("gcal_updated", "gcal_updated"),
        )

        cm_raw = m.get("color_mapping", {}) or {}
        cm_by_color = dict(_DEFAULT_NOTION_COLOR_TO_GCAL)
        cm_by_color.update({k: int(v) for k, v in (cm_raw.get("by_notion_color") or {}).items()})
        cm = ColorMapping(
            source_property=cm_raw.get("source_property", "") or "",
            by_name={k: int(v) for k, v in (cm_raw.get("by_name") or {}).items()},
            by_notion_color=cm_by_color,
            default_color_id=int(cm_raw.get("default_color_id", 0)),
        )

        wp_raw = m.get("weekly_properties", {}) or {}
        wp = WeeklyPropertyNames(
            title=wp_raw.get("title", "이름"),
            weekday=wp_raw.get("weekday", "요일"),
            start_time=wp_raw.get("start_time", "시작 시간"),
            end_time=wp_raw.get("end_time", "종료 시간"),
            active=wp_raw.get("active", "활성"),
            end_date=wp_raw.get("end_date") or None,
            gcal_event_id=wp_raw.get("gcal_event_id", "gcal_event_id"),
            gcal_updated=wp_raw.get("gcal_updated", "gcal_updated"),
        )

        mappings.append(Mapping(
            name=m["name"],
            notion_database_id=_normalize_id(m["notion_database_id"]),
            google_calendar_id=m["google_calendar_id"],
            properties=props,
            color_mapping=cm,
            event_title_prefix=m.get("event_title_prefix", ""),
            kind=m.get("kind", "events"),
            weekly_properties=wp,
            timezone=m.get("timezone", "Asia/Seoul"),
            weekly_lookahead_weeks=int(m.get("weekly_lookahead_weeks", 2)),
            target_events_mapping=m.get("target_events_mapping", ""),
            target_source_property=m.get("target_source_property", "주간출처"),
        ))

    opts_raw = raw.get("options", {}) or {}
    opts = Options(
        past_days=int(opts_raw.get("past_days", 30)),
        future_days=int(opts_raw.get("future_days", 365)),
        conflict_resolution=opts_raw.get("conflict_resolution", "last_write_wins"),
        dry_run=bool(opts_raw.get("dry_run", False)),
        log_level=opts_raw.get("log_level", "INFO"),
        delete_gcal_when_notion_archived=bool(opts_raw.get("delete_gcal_when_notion_archived", False)),
        archive_notion_when_gcal_deleted=bool(opts_raw.get("archive_notion_when_gcal_deleted", False)),
        create_notion_from_gcal=bool(opts_raw.get("create_notion_from_gcal", False)),
    )
    return Config(mappings=mappings, options=opts)


def _normalize_id(raw: str) -> str:
    """하이픈 있든 없든 Notion API가 받는 형태로 정규화."""
    s = raw.replace("-", "").strip()
    if len(s) != 32:
        return raw  # let API validate
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"
