# HansolCal

Notion ↔ Google Calendar **양방향** 동기화. 5분 간격, 개수 제한 없음.
GitHub Actions cron으로 돌고, 상태는 Notion DB에만 저장 (외부 DB 불필요).

## 동작

- 여러 Notion DB ↔ 여러 Google Calendar를 매핑 가능
- 추가/수정/삭제 양쪽 모두 반영 (last-write-wins 충돌 해결)
- 한 이벤트 = 한 Notion page ↔ 한 GCal event 짝 (ID로 매핑)
- 반복 이벤트는 현재 미지원 (Notion은 `singleEvents` 형태로 저장 권장)

## Notion DB에 필요한 property

| 이름 (기본값) | 타입 | 용도 |
|---|---|---|
| `Name` | Title | 이벤트 제목 |
| `Date` | Date (start+end 둘 다 한 property) | 이벤트 시간 |
| `Description` | Rich text | 설명 (선택) |
| `Location` | Rich text | 장소 (선택) |
| `gcal_event_id` | Rich text | **필수 — 동기화 매핑 저장용** |
| `gcal_updated` | Rich text | **필수 — 마지막 sync 시각** |

이름은 `config.yaml`에서 변경 가능.

## 초기 셋업 (한 번만)

### 1. Python 환경
```bash
cd ~/HansolCal
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Notion Integration 토큰
1. https://www.notion.so/my-integrations → New integration
2. 토큰 복사 → `export NOTION_TOKEN=secret_xxxxx`
3. 동기화할 Notion DB에 integration 초대 (DB 페이지 오른쪽 상단 ⋯ → Connections → Add)

### 3. Google OAuth 자격증명
1. https://console.cloud.google.com → 새 프로젝트
2. "APIs & Services" → "Library" → "Google Calendar API" 활성화
3. "Credentials" → "Create credentials" → "OAuth client ID"
   - Application type: **Desktop app**
4. JSON 다운로드 → 이 디렉토리에 `google_credentials.json` 으로 저장
5. OAuth consent screen → Test users에 본인 이메일 추가
6. 토큰 발급:
```bash
python get_google_token.py
```
→ 브라우저 열려서 승인하면 `google_token.json` 생성

### 4. config.yaml 작성
```bash
cp config.example.yaml config.yaml
# 에디터로 열어서 notion_database_id, google_calendar_id 수정
```

- `notion_database_id`: Notion DB URL의 `...notion.so/xxx?v=...` 중 `xxx` 부분 (32자 hex)
- `google_calendar_id`: "primary" 또는 Calendar 설정 → "Integrate calendar" → Calendar ID

### 5. 로컬 테스트
```bash
# 먼저 dry_run=true로
python main.py
# 문제 없으면 config.yaml에서 dry_run=false로 변경
python main.py
```

## GitHub Actions로 올리기 (5분 cron)

### 1. Private repo 생성 후 push
```bash
cd ~/HansolCal
git add .
git commit -m "init"
gh repo create HansolCal --private --source=. --push
```

### 2. Secrets 등록
Repo Settings → Secrets and variables → Actions → New repository secret

| 이름 | 값 |
|---|---|
| `NOTION_TOKEN` | `secret_xxxxx` (위 2번) |
| `GOOGLE_TOKEN_JSON` | `google_token.json` 파일 **내용 전체** (JSON) |
| `CONFIG_YAML` | `config.yaml` 파일 **내용 전체** |

### 3. Actions 탭에서 동작 확인
- `workflow_dispatch`로 수동 실행 한 번 → 로그 확인
- 정상이면 5분 cron이 알아서 돌아감

## 주의

- GitHub Actions cron은 ~5분 정확히 보장 안 됨 (트래픽 시 10~15분 지연 가능). 본인 일정 관리용이면 충분.
- GCal events에는 `extendedProperties.private.notion_page_id` 가 삽입됨. 삭제하면 재매칭 안 됨.
- Notion에서 page **archive** 시 다음 사이클에 GCal 이벤트 자동 삭제됨.
- GCal에서 이벤트 삭제 시 Notion page는 **archive되지 않음** (데이터 보존 우선). 바꾸고 싶으면 `sync.py`의 3단계 로직 수정.
