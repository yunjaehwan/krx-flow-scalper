# -*- coding: utf-8 -*-
"""/opt/krx-bot/krx_realtime_autotrader.py (commit ab0266b) 에서 휴장일·API 백오프(서킷브레이커)·텔레그램 함수만
그대로 복사한 모듈. 주문·잔고·계좌 관련 코드는 가져오지 않았다.
바뀐 것은 아래 상수뿐: 실전 서버 주소(시세 조회 전용), 이 봇 전용 상태 폴더."""
import json
import os
import time
import datetime
from pathlib import Path

import requests

KST = datetime.timezone(datetime.timedelta(hours=9))
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"   # 실전 서버 — 시세·휴장일 조회만 사용
STATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "state")
HOLIDAY_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "krx_holidays.txt")
HOLIDAY_CHECK_FROM = "09:05"
HOLIDAY_CONFIRM_BY = "09:30"
TRADING_DAY_CHECK_CODES = ("005930", "000660")
MARKET_CLOSE = "15:30"

def now_kst():
    return datetime.datetime.now(KST)


class ApiBackoff:
    def __init__(self, name, base=1.0, max_delay=60.0, alert_after=10, clock=time.time):
        self.name, self.base, self.max_delay, self.alert_after, self.clock = name, base, max_delay, alert_after, clock
        self.fails, self.next_at, self.alerted = 0, 0.0, False

    def allowed(self):
        return self.clock() >= self.next_at

    def failure(self, err):
        self.fails += 1
        delay = min(self.max_delay, self.base * 2 ** (self.fails - 1))
        self.next_at = self.clock() + delay
        if self.fails & (self.fails - 1) == 0:          # 1, 2, 4, 8, ... 번째 실패만 로그 (로그 폭주 방지)
            print(f"[{self.name} 실패 {self.fails}회] {err} / 다음 시도까지 {delay:.0f}초")
        if self.fails >= self.alert_after and not self.alerted:
            self.alerted = True
            send_telegram(f"[경고] {self.name}가 {self.fails}회 연속 실패했습니다. 재시도 간격을 늘려 계속 시도합니다.\n마지막 오류: {err}")

    def success(self):
        if self.alerted:
            send_telegram(f"[복구] {self.name}가 {self.fails}회 실패 후 다시 정상화됐습니다.")
        self.fails, self.next_at, self.alerted = 0, 0.0, False


def load_holidays(path=HOLIDAY_FILE):
    days = set()
    if os.path.exists(path):
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            s = line.split("#", 1)[0].strip()
            if len(s) == 8 and s.isdigit():
                days.add(s)
    return days


def latest_trading_date(app_key, app_secret, token, code):
    """일별시세의 가장 최근 거래일(YYYYMMDD). 조회 실패면 None."""
    try:
        r = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers={"content-type": "application/json", "authorization": f"Bearer {token}",
                     "appkey": app_key, "appsecret": app_secret, "tr_id": "FHKST01010400", "custtype": "P"},
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                    "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"},
            timeout=10,
        )
        rows = r.json().get("output") or []
        dates = [x.get("stck_bsop_date", "") for x in rows if x.get("stck_bsop_date")]
        return max(dates) if dates else None
    except Exception as e:
        print(f"[거래일 확인 조회 실패] {code}: {e}")
        return None


def trading_day_status(now, holidays, latest_dates):
    """'holiday' | 'trading' | 'unknown'. latest_dates: 기준 종목들의 최신 거래일 목록(None 포함 가능)."""
    today = now.strftime("%Y%m%d")
    hm = now.strftime("%H:%M")
    if now.weekday() >= 5 or today in holidays:
        return "holiday"
    valid = [d for d in latest_dates if d]
    if any(d == today for d in valid):
        return "trading"
    if hm >= HOLIDAY_CONFIRM_BY and valid and all(d < today for d in valid):
        return "holiday"
    return "unknown"


def kis_open_day(app_key, app_secret, token, day):
    """KIS 국내휴장일조회로 day(YYYYMMDD)가 개장일인지 확인.
    return: (True=개장일 | False=휴장일 | None=API 사용 불가·실패, 사유 문자열).
    같은 날짜는 결과(실패 포함)를 캐시해서 하루 1회만 호출한다."""
    cache_path = os.path.join(STATE_DIR, "holiday_api_cache.json")
    try:
        cache = json.loads(Path(cache_path).read_text(encoding="utf-8"))
        if cache.get("date") == day:
            return cache.get("open"), cache.get("reason", "") + " (오늘 조회 결과 재사용)"
    except Exception:
        pass
    result, reason = None, ""
    try:
        r = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/chk-holiday",
            headers={"content-type": "application/json", "authorization": f"Bearer {token}",
                     "appkey": app_key, "appsecret": app_secret, "tr_id": "CTCA0903R", "custtype": "P"},
            params={"BASS_DT": day, "CTX_AREA_FK": "", "CTX_AREA_NK": ""},
            timeout=10,
        )
        j = r.json()
        if j.get("rt_cd") == "0":
            row = next((x for x in (j.get("output") or []) if x.get("bass_dt") == day), None)
            if row is None:
                reason = "응답에 오늘 날짜가 없음"
            else:
                result = row.get("opnd_yn") == "Y"
                reason = f"opnd_yn={row.get('opnd_yn')}"
        else:
            reason = f"{j.get('msg_cd')} {j.get('msg1')}"
    except Exception as e:
        reason = f"호출 실패: {e}"
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        Path(cache_path).write_text(json.dumps({"date": day, "open": result, "reason": reason,
                                                "checked_at": now_kst().isoformat()}, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"[휴장일 API 캐시 저장 실패] {e}")
    return result, reason


def holiday_file_reminder(now, holidays):
    """API를 못 쓰는 상태에서 12월인데 목록에 내년 날짜가 없으면 갱신 알림 문구(로그용), 아니면 None."""
    nxt = str(now.year + 1)
    if now.month == 12 and not any(d.startswith(nxt) for d in holidays):
        return (f"[알림] KIS 휴장일 API를 쓸 수 없어 {HOLIDAY_FILE} 목록으로 휴장일을 판단하고 있습니다. "
                f"{nxt}년 휴장일이 목록에 없습니다 — KRX 휴장일 목록을 보고 연말 전에 추가해 주세요.")
    return None


def holiday_marker_path(day):
    return os.path.join(STATE_DIR, f"holiday_{day}")


def send_telegram(message):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                       data={"chat_id": chat_id, "text": message}, timeout=10)
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")
