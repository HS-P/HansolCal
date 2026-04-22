"""로컬에서 1회 실행 — Google OAuth로 google_token.json 생성.

사전 준비:
  1. Google Cloud Console에서 OAuth 2.0 Client (Desktop 타입) 생성
  2. 다운로드한 JSON을 이 디렉토리에 google_credentials.json 으로 저장
  3. python get_google_token.py 실행 → 브라우저 열려서 승인
  4. 생성된 google_token.json 내용을 GitHub secrets의 GOOGLE_TOKEN_JSON 에 통째로 붙여넣기
"""
from __future__ import annotations

import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/calendar"]


def main() -> int:
    creds_path = Path("google_credentials.json")
    if not creds_path.exists():
        print("ERROR: google_credentials.json not found.", file=sys.stderr)
        print("  Google Cloud Console → APIs & Services → Credentials 에서", file=sys.stderr)
        print("  'OAuth 2.0 Client ID' (Application type: Desktop) 생성 후 JSON 다운로드", file=sys.stderr)
        return 1

    flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
    creds = flow.run_local_server(port=0)

    token_path = Path("google_token.json")
    token_path.write_text(creds.to_json())
    print(f"✓ Wrote {token_path}")
    print("")
    print("다음 스텝:")
    print(f"  1. 로컬 테스트: python main.py")
    print(f"  2. GitHub secrets에 GOOGLE_TOKEN_JSON 추가 (값: {token_path} 의 내용 전체)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
