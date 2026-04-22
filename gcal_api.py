"""Google Calendar API wrapper — 최소 기능 집합.

OAuth 토큰은 google_token.json에 보관. 최초 1회 get_google_token.py 로 발급.
GitHub Actions에서는 GOOGLE_TOKEN_JSON 환경변수로 주입.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from dateutil import parser as dtparser
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Notion page ID를 GCal event의 extendedProperties.private.notion_page_id에 저장
EXTPROP_NOTION_PAGE_ID = "notion_page_id"


@dataclass
class GCalEvent:
    event_id: str
    title: str
    start: datetime
    end: datetime | None
    all_day: bool
    description: str
    location: str
    updated: datetime
    notion_page_id: str  # extendedProperties에서 추출. 없으면 ""
    color_id: int = 0    # 0 = GCal 기본, 1~11 = 색 지정
    raw: dict[str, Any] = field(default_factory=dict)


class GCalAPI:
    def __init__(self, token_path: str = "google_token.json"):
        creds = _load_credentials(token_path)
        self.service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    def list_events(self, calendar_id: str, time_min: datetime, time_max: datetime) -> list[GCalEvent]:
        events: list[GCalEvent] = []
        page_token: str | None = None
        while True:
            resp = self.service.events().list(
                calendarId=calendar_id,
                timeMin=_iso_z(time_min),
                timeMax=_iso_z(time_max),
                singleEvents=True,
                maxResults=2500,
                pageToken=page_token,
                showDeleted=False,
            ).execute()
            for item in resp.get("items", []):
                if item.get("status") == "cancelled":
                    continue
                ev = _parse_event(item)
                if ev is not None:
                    events.append(ev)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return events

    def create_event(self, calendar_id: str, *, title: str, start: datetime, end: datetime | None,
                     all_day: bool, description: str, location: str, notion_page_id: str,
                     color_id: int = 0) -> GCalEvent:
        body = _to_gcal_body(title, start, end, all_day, description, location, notion_page_id, color_id)
        item = self.service.events().insert(calendarId=calendar_id, body=body).execute()
        return _parse_event(item)  # type: ignore[return-value]

    def update_event(self, calendar_id: str, event_id: str, *, title: str, start: datetime,
                     end: datetime | None, all_day: bool, description: str, location: str,
                     notion_page_id: str, color_id: int = 0) -> GCalEvent:
        body = _to_gcal_body(title, start, end, all_day, description, location, notion_page_id, color_id)
        item = self.service.events().update(calendarId=calendar_id, eventId=event_id, body=body).execute()
        return _parse_event(item)  # type: ignore[return-value]

    def create_recurring_event(self, calendar_id: str, *, title: str, start_dt: datetime,
                               end_dt: datetime, timezone_name: str, rrule: str,
                               description: str, location: str, notion_page_id: str,
                               color_id: int = 0) -> "GCalEvent":
        body = _to_recurring_body(title, start_dt, end_dt, timezone_name, rrule,
                                  description, location, notion_page_id, color_id)
        item = self.service.events().insert(calendarId=calendar_id, body=body).execute()
        return _parse_event(item)  # type: ignore[return-value]

    def update_recurring_event(self, calendar_id: str, event_id: str, *, title: str,
                               start_dt: datetime, end_dt: datetime, timezone_name: str,
                               rrule: str, description: str, location: str,
                               notion_page_id: str, color_id: int = 0) -> "GCalEvent":
        body = _to_recurring_body(title, start_dt, end_dt, timezone_name, rrule,
                                  description, location, notion_page_id, color_id)
        item = self.service.events().update(calendarId=calendar_id, eventId=event_id, body=body).execute()
        return _parse_event(item)  # type: ignore[return-value]

    def delete_event(self, calendar_id: str, event_id: str) -> None:
        try:
            self.service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        except Exception as e:
            # 이미 삭제된 경우 무시
            if "410" in str(e) or "404" in str(e):
                return
            raise

    def get_event(self, calendar_id: str, event_id: str) -> GCalEvent | None:
        try:
            item = self.service.events().get(calendarId=calendar_id, eventId=event_id).execute()
            if item.get("status") == "cancelled":
                return None
            return _parse_event(item)
        except Exception as e:
            if "404" in str(e) or "410" in str(e):
                return None
            raise


# ────────────────────────────────────────
# Helpers
# ────────────────────────────────────────
def _load_credentials(token_path: str) -> Credentials:
    """GOOGLE_TOKEN_JSON 환경변수 > token_path 파일 순으로 로드. 만료 시 refresh."""
    info = None
    env_json = os.environ.get("GOOGLE_TOKEN_JSON")
    if env_json:
        info = json.loads(env_json)
    else:
        p = Path(token_path).expanduser()
        if p.exists():
            info = json.loads(p.read_text())
    if info is None:
        raise RuntimeError(
            "Google token not found. Run: python get_google_token.py"
        )

    creds = Credentials.from_authorized_user_info(info, SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            # 파일 백업 (로컬 실행 시)
            if env_json is None:
                Path(token_path).expanduser().write_text(creds.to_json())
        else:
            raise RuntimeError("Invalid Google credentials; re-run get_google_token.py")
    return creds


def _parse_event(item: dict) -> GCalEvent | None:
    ext = item.get("extendedProperties", {}) or {}
    priv = ext.get("private", {}) or {}
    notion_page_id = priv.get(EXTPROP_NOTION_PAGE_ID, "")

    start_obj = item.get("start", {}) or {}
    end_obj = item.get("end", {}) or {}
    if "dateTime" in start_obj:
        start = dtparser.isoparse(start_obj["dateTime"])
        all_day = False
    elif "date" in start_obj:
        start = dtparser.isoparse(start_obj["date"])
        all_day = True
    else:
        return None

    end: datetime | None = None
    if "dateTime" in end_obj:
        end = dtparser.isoparse(end_obj["dateTime"])
    elif "date" in end_obj:
        end = dtparser.isoparse(end_obj["date"])

    # GCal all-day 이벤트의 end는 exclusive. end == start + 1일이면 당일치기로 정규화.
    # 이렇게 해야 Notion(end=null, 당일치기)과 비교/동기화 시 같게 판단됨.
    if all_day and end is not None:
        from datetime import timedelta
        if (end - start) == timedelta(days=1):
            end = None

    updated = dtparser.isoparse(item["updated"]) if item.get("updated") else datetime.now()

    color_id_raw = item.get("colorId", "")
    try:
        color_id = int(color_id_raw) if color_id_raw else 0
    except (TypeError, ValueError):
        color_id = 0

    return GCalEvent(
        event_id=item["id"],
        title=item.get("summary", ""),
        start=start,
        end=end,
        all_day=all_day,
        description=item.get("description", "") or "",
        location=item.get("location", "") or "",
        updated=updated,
        notion_page_id=notion_page_id,
        color_id=color_id,
        raw=item,
    )


def _to_gcal_body(title: str, start: datetime, end: datetime | None, all_day: bool,
                  description: str, location: str, notion_page_id: str, color_id: int = 0) -> dict:
    def _time_block(d: datetime) -> dict:
        if all_day:
            return {"date": d.strftime("%Y-%m-%d")}
        return {"dateTime": d.isoformat()}

    if end is None:
        from datetime import timedelta
        end = start + (timedelta(days=1) if all_day else timedelta(hours=1))

    body: dict[str, Any] = {
        "summary": title or "(untitled)",
        "start": _time_block(start),
        "end": _time_block(end),
        "description": description or "",
        "location": location or "",
        "extendedProperties": {
            "private": {EXTPROP_NOTION_PAGE_ID: notion_page_id}
        },
    }
    if color_id and 1 <= color_id <= 11:
        body["colorId"] = str(color_id)
    return body


def _to_recurring_body(title: str, start_dt: datetime, end_dt: datetime, timezone_name: str,
                       rrule: str, description: str, location: str, notion_page_id: str,
                       color_id: int = 0) -> dict:
    """Recurring event body (timed, with timezone).

    rrule 예: "FREQ=WEEKLY;BYDAY=MO,WE" (RRULE: prefix는 자동 추가).
    """
    body: dict[str, Any] = {
        "summary": title or "(untitled)",
        "start": {"dateTime": start_dt.isoformat(), "timeZone": timezone_name},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": timezone_name},
        "recurrence": [f"RRULE:{rrule}"],
        "description": description or "",
        "location": location or "",
        "extendedProperties": {
            "private": {EXTPROP_NOTION_PAGE_ID: notion_page_id}
        },
    }
    if color_id and 1 <= color_id <= 11:
        body["colorId"] = str(color_id)
    return body


def _iso_z(d: datetime) -> str:
    """timeMin/timeMax는 RFC3339 with Z or offset."""
    if d.tzinfo is None:
        # naive는 UTC로 가정
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.isoformat()
