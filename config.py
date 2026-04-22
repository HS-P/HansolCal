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
class Mapping:
    name: str
    notion_database_id: str
    google_calendar_id: str
    properties: PropertyNames
    color_mapping: ColorMapping = field(default_factory=ColorMapping)
    event_title_prefix: str = ""


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

        mappings.append(Mapping(
            name=m["name"],
            notion_database_id=_normalize_id(m["notion_database_id"]),
            google_calendar_id=m["google_calendar_id"],
            properties=props,
            color_mapping=cm,
            event_title_prefix=m.get("event_title_prefix", ""),
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
