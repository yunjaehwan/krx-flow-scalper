# -*- coding: utf-8 -*-
"""
KRX 수급 단타 — 실전 시세 + 가상 체결 기록 봇
=============================================
- 실전 키는 **시세 조회 전용**. 이 파일에는 주문·잔고·계좌 API가 없다.
  REST 호출은 KisRest.get() 하나로만 나가고, 허용 경로(시세·순위)가 아니면 예외를 낸다.
- 기존 추격봇(/opt/krx-bot)과 완전히 분리: 다른 폴더·다른 사용자(krxflow)·다른 키(실전 조회)·다른 상태 파일.
  휴장일·API 백오프·텔레그램(vendor/krx_common.py)과 웹소켓(vendor/krx_realtime_ws.py)은 기존 코드 복사본을 재사용한다.

흐름
----
스캐너(REST, 호출 한도 20건/초의 70% = 14건/초 이내)
  → 후보: 시총 1000억↑, 관리·정리매매·거래정지 제외, 등락률 +3%↑, 거래량 급증(거래증가율) 순위 상위 30(시장별),
          그리고 (종목별 프로그램 당일 누적 순매수 > 0  또는  외국인·기관 가집계 순매수 > 0, 가집계는 09:31 이후 있으면 참고)
  → 감시: 웹소켓 41건 = 지수 2(KOSPI·KOSDAQ) + 종목 체결 최대 39. 식거나(+3% 미만) 신호가 끝난 종목은 빼고 교체
  → 신호: 등락률 5~12%, 3분 거래량 ≥ 평소 3분×2, VWAP 위·괴리 +2% 이내, 프로그램 5분·누적 순매수 > 0,
          체결강도 ≥ 120, 해당 시장 지수 > 시가, 09:00~14:30, 종목당 하루 1회
  → 가상 체결: 매수=매도1호가, 매도=매수1호가, 왕복 0.45%
  → 청산: 고점 -3% 추적손절, 15:20 가격이 당일 고가 -2% 이내면 다음날 시가(첫 체결의 매수1호가), 아니면 15:20 매도
  → 자금: 가상 1,000만 원, 종목당 25%, 최대 2종목, 일일 -2%면 전량 청산 후 중단
장 마감 후: 지수 1분봉 저장, 전략 신호·그림자 신호를 같은 규칙으로 분봉 사후 계산, 텔레그램 요약, API 갱신 주기 로그
"""
import argparse
import csv
import datetime
import json
import os
import sys
import threading
import time
from collections import deque
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / "vendor"))
import krx_common as C                                   # noqa: E402  (기존 봇 함수 복사본)
from krx_realtime_ws import (KISRealtimeWS, RealtimeBook, PRICE_FIELDS,   # noqa: E402
                             WS_STALE_TIMEOUT_SEC, get_approval_key, split_records)
import flow_core as F                                    # noqa: E402

REAL_BASE = "https://openapi.koreainvestment.com:9443"
ALLOWED_PATHS = ("/uapi/domestic-stock/v1/quotations/", "/uapi/domestic-stock/v1/ranking/")
RATE_PER_SEC = 14                     # 실측 한도 20건/초의 70%
WS_LIMIT, INDEX_CODES = 41, {"KOSPI": "0001", "KOSDAQ": "1001"}
STOCK_SLOTS = WS_LIMIT - len(INDEX_CODES)
RANK_EVERY, ELIGIBLE_PROG_EVERY, WATCH_PROG_EVERY, EST_EVERY = 5, 20, 10, 300
PROG_FRESH_SEC = 30                   # 프로그램 값이 이보다 오래되면 신호 판단에 쓰지 않음
TICK_FRESH_SEC = 60
EVAL_EVERY = 1.0                      # 신호 판단 주기(초). 보유 종목 손절은 틱마다 즉시 처리
EST_VALID_FROM = "093100"             # 가집계 첫 발표(09:30) 이전 값은 전날 값일 수 있어 쓰지 않음
EXCL = "0110011101"                   # 제외: 관리종목·정리매매·거래정지·ETF·ETN·SPAC (투자주의·우선주·불성실공시·신용불가는 포함)
TAG = "[수급단타·가상]"

STATE_DIR, DATA_DIR = BASE_DIR / "state", BASE_DIR / "data"
STATE_FILE = STATE_DIR / "state.json"
TOKEN_FILE = STATE_DIR / "kis_token.json"


def now():
    return C.now_kst()


def today_str():
    return now().strftime("%Y%m%d")


def hms_now():
    return now().strftime("%H%M%S")


def epoch_of(day, hms):
    d = datetime.datetime.strptime(day + hms[:6], "%Y%m%d%H%M%S").replace(tzinfo=C.KST)
    return d.timestamp()


def fnum(v, default=0.0):
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return default


def log(msg):
    print(f"[{now():%H:%M:%S}] {msg}", flush=True)


_send_telegram = C.send_telegram
C.send_telegram = lambda msg: _send_telegram(f"{TAG} {msg}")   # 백오프 경고 등 복사해 온 함수의 알림에도 머리말


def telegram(msg):
    C.send_telegram(msg)


# ============================================================
# REST (조회 전용)
# ============================================================
class ApiError(Exception):
    pass


class RateLimiter:
    """최근 1초 안의 호출 수를 rate 이하로 유지(모든 스레드 공용)."""

    def __init__(self, rate):
        self.rate, self.lock, self.times = rate, threading.Lock(), deque()

    def wait(self):
        while True:
            with self.lock:
                t = time.monotonic()
                while self.times and t - self.times[0] >= 1.0:
                    self.times.popleft()
                if len(self.times) < self.rate:
                    self.times.append(t)
                    return
            time.sleep(0.02)


class KisRest:
    def __init__(self, key, secret):
        self.key, self.secret = key, secret
        self.limiter = RateLimiter(RATE_PER_SEC)
        self.lock = threading.Lock()
        self._tok = None

    def token(self, force=False):
        with self.lock:
            if not force and self._tok and self._tok[1] > time.time():
                return self._tok[0]
            if not force and TOKEN_FILE.exists():
                try:
                    t = json.loads(TOKEN_FILE.read_text())
                    if t["exp"] > time.time():
                        self._tok = (t["tok"], t["exp"])
                        return t["tok"]
                except Exception:
                    pass
            r = requests.post(f"{REAL_BASE}/oauth2/tokenP", json={"grant_type": "client_credentials",
                                                                  "appkey": self.key, "appsecret": self.secret}, timeout=10)
            tok = r.json().get("access_token")
            if not tok:
                raise ApiError(f"토큰 발급 실패 HTTP {r.status_code}")
            exp = time.time() + 20 * 3600
            TOKEN_FILE.write_text(json.dumps({"tok": tok, "exp": exp}))
            os.chmod(TOKEN_FILE, 0o600)
            self._tok = (tok, exp)
            return tok

    def get(self, path, tr_id, params):
        if not path.startswith(ALLOWED_PATHS):
            raise ApiError(f"허용되지 않은 경로(조회 전용 봇): {path}")
        for attempt in (1, 2):
            self.limiter.wait()
            h = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {self.token(force=attempt == 2)}",
                 "appkey": self.key, "appsecret": self.secret, "tr_id": tr_id, "custtype": "P"}
            try:
                r = requests.get(REAL_BASE + path, headers=h, params=params, timeout=10)
                j = r.json()
            except Exception as e:
                raise ApiError(f"{tr_id} 호출 실패: {type(e).__name__}")
            if j.get("rt_cd") == "0":
                return j
            if j.get("msg_cd") in ("EGW00123", "EGW00121") and attempt == 1:     # 토큰 만료·무효 → 한 번 재발급
                continue
            raise ApiError(f"{tr_id} {j.get('msg_cd')} {j.get('msg1')}")


def rows_of(j, *keys):
    for k in keys or ("output", "output1", "output2"):
        v = j.get(k)
        if v:
            return v if isinstance(v, list) else [v]
    return []


# ============================================================
# 웹소켓: 종목 체결(H0STCNT0) + 지수(H0UPCNT0), 동적 등록/해제
# ============================================================
INDEX_FIELDS = ["bstp_cls_code", "bsop_hour", "prpr_nmix", "prdy_vrss_sign", "bstp_nmix_prdy_vrss", "acml_vol",
                "acml_tr_pbmn", "pcas_vol", "pcas_tr_pbmn", "prdy_ctrt", "oprc_nmix", "nmix_hgpr", "nmix_lwpr"]
INDEX_NFIELDS = 30


class FlowWS(KISRealtimeWS):
    """기존 KISRealtimeWS(재접속·백오프·좀비 감지)를 그대로 쓰고, 등록 대상만 바꾼다."""

    def __init__(self, key, secret, book, on_tick, on_index):
        super().__init__(key, secret, is_mock=False, book=book)
        self.on_tick, self.on_index = on_tick, on_index
        self.sub_lock = threading.Lock()
        self.approval = None
        self.sub_status = {}

    def _send(self, tr, key, typ):
        ws = self.ws
        if ws is None or not self.connected.is_set() or not self.approval:
            return False
        try:
            ws.send(self._message(tr, key, typ, self.approval))
            return True
        except Exception as e:
            log(f"[WS] 전송 실패 {tr} {key}: {e}")
            return False

    def _subscribe_all(self, ws):
        # 부모 _on_open 이 connected·유예 시각을 정하고, 여기서 예외가 나면 소켓을 닫아 재연결한다
        self.approval = get_approval_key(self.app_key, self.app_secret, False)
        for code in INDEX_CODES.values():
            ws.send(self._message("H0UPCNT0", code, "1", self.approval)); time.sleep(0.05)
        with self.sub_lock:
            codes = sorted(self.stock_codes)
        for code in codes:
            ws.send(self._message("H0STCNT0", code, "1", self.approval)); time.sleep(0.05)
        log(f"[WS] 연결·등록: 지수 {len(INDEX_CODES)} + 종목 {len(codes)}")

    def subscribe(self, code):
        with self.sub_lock:
            if code in self.stock_codes:
                return
            self.stock_codes.add(code)
        self._send("H0STCNT0", code, "1")

    def unsubscribe(self, code):
        with self.sub_lock:
            if code not in self.stock_codes:
                return
            self.stock_codes.discard(code)
        self._send("H0STCNT0", code, "2")

    def watched(self):
        with self.sub_lock:
            return set(self.stock_codes)

    def _on_message(self, ws, message):
        self.last_message_at = time.time()
        if not message:
            return
        if message[:2] in ("0|", "1|"):
            # 워치독은 시세만 센다(PINGPONG 제외). last_tick_at 은 재연결로 초기화되지 않는 시세 공백 경고 기준
            self.last_data_at = self.last_tick_at = time.time()
        if message.startswith("0|"):
            parts = message.split("|", 3)
            if len(parts) < 4:
                return
            tr, vals = parts[1], parts[3].split("^")
            try:
                cnt = int(parts[2])
            except ValueError:
                cnt = 0
            if tr == "H0STCNT0":
                for rec in split_records(vals, cnt, len(PRICE_FIELDS)):
                    row = dict(zip(PRICE_FIELDS, rec))
                    self.book.update_trade(row)
                    try:
                        self.on_tick(row)
                    except Exception as e:
                        log(f"[틱 처리 오류] {e}")
            elif tr == "H0UPCNT0":
                for rec in split_records(vals, cnt, INDEX_NFIELDS):
                    row = dict(zip(INDEX_FIELDS, rec))
                    try:
                        self.on_index(row)
                    except Exception as e:
                        log(f"[지수 처리 오류] {e}")
            return
        if message.startswith("1|"):
            return
        try:
            obj = json.loads(message)
        except Exception:
            return
        hd = obj.get("header", {})
        if hd.get("tr_id") == "PINGPONG":
            ws.send(message)
            return
        msg = obj.get("body", {}).get("msg1", "")
        key = (hd.get("tr_id"), hd.get("tr_key"))
        self.sub_status[key] = msg
        if "MAX SUBSCRIBE OVER" in msg.upper() and hd.get("tr_id") == "H0STCNT0":
            with self.sub_lock:
                self.stock_codes.discard(hd.get("tr_key"))
            log(f"[WS] 등록 한도 초과로 제외: {hd.get('tr_key')}")
        elif msg and "SUCCESS" not in msg.upper():
            log(f"[WS] {key}: {msg}")


# ============================================================
# CSV 기록
# ============================================================
IND_FIELDS = ["chg", "vol3m", "normal3m", "vol_ratio", "price", "vwap", "vwap_gap_pct", "prog5", "prog_cum", "prog_age",
              "cttr", "idx_now", "idx_open", "idx_pct", "est_gb", "est_sum", "vol_inrt", "cap_eok", "ask1", "bid1", "day_high"]
BASE_FIELDS = ["date", "code", "name", "market", "sig_time"]
TRADE_FIELDS = BASE_FIELDS + ["entry_time", "entry", "qty", "exit_date", "exit_time", "exit", "reason", "ret_pct", "pnl_won",
                              "max_px", "min_px", "max_pct", "min_pct"] + IND_FIELDS
SIGNAL_FIELDS = BASE_FIELDS + ["action"] + IND_FIELDS
SHADOW_FIELDS = BASE_FIELDS + ["flow_ok", "fail_flow"] + IND_FIELDS
SIM_FIELDS = ["kind", "date", "code", "name", "sig_time", "entry", "status", "exit_date", "exit_time", "exit", "reason",
              "ret_pct", "max_pct", "min_pct"]


def append_csv(name, fields, row):
    path = DATA_DIR / name
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerow({k: (round(v, 4) if isinstance(v, float) else v) for k, v in row.items()})


def read_csv(name):
    path = DATA_DIR / name
    if not path.exists():
        return []
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# ============================================================
# 봇
# ============================================================
class Bot:
    def __init__(self, key, secret):
        self.rest = KisRest(key, secret)
        self.lock = threading.RLock()            # 계좌·상태
        self.dlock = threading.RLock()           # 후보·프로그램·지수 데이터
        self.running = True
        self.day = today_str()
        self.book = RealtimeBook()
        self.ws = FlowWS(key, secret, self.book, self.on_tick, self.on_index)
        self.cand = {}          # code -> 후보 정보(스캐너)
        self.status = {}        # code -> (정상여부, 사유)  (현재가 조회 캐시, 하루)
        self.prog = {}          # code -> ProgramSeries
        self.prog_at = {}       # code -> 마지막 프로그램 조회 성공 epoch
        self.est = {}           # code -> (gb, sum, epoch)
        self.idx = {}           # "KOSPI" -> dict(now, open, t)
        self.idx_bars = {m: F.MinuteBars() for m in INDEX_CODES}
        self.day_high = {}      # code -> 당일 고가(웹소켓)
        self.refresh = F.RefreshTracker()
        self.prog_lags = []
        self.closed = []        # 틱 스레드에서 청산된 거래(메인 스레드가 기록)
        self.backoff = {k: C.ApiBackoff(n) for k, n in (("rank", "순위 조회"), ("prog", "프로그램 매매 조회"),
                                                        ("est", "가집계 조회"), ("price", "현재가 조회"))}
        self.load_state()

    # ---------------- 상태 ----------------
    def load_state(self):
        s = {}
        if STATE_FILE.exists():
            s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        a = s.get("account") or {}
        self.acct = F.VirtualAccount(a.get("cash", F.START_CASH), a.get("positions") or {})
        self.st = s
        if s.get("date") != self.day:                      # 새 거래일
            prices = {c: p["last"] for c, p in self.acct.positions.items()}
            self.st = dict(date=self.day, day_start_equity=self.acct.equity(prices), halted=False, signaled=[],
                           shadowed=[], force_done=False, post_close_done=s.get("post_close_done", ""),
                           morning_done="", pending_sims=s.get("pending_sims", []), shadow_open={},
                           signal_open={})
            self.save_state()

    def save_state(self):
        with self.lock:
            self.st["account"] = self.acct.to_json()
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.st, ensure_ascii=False, indent=1), encoding="utf-8")
            os.replace(tmp, STATE_FILE)

    # ---------------- 웹소켓 콜백 (수신 스레드) ----------------
    def on_tick(self, row):
        code, hms = row.get("MKSC_SHRN_ISCD", ""), row.get("STCK_CNTG_HOUR", "")
        px, bid, hi = fnum(row.get("STCK_PRPR")), fnum(row.get("BIDP1")), fnum(row.get("STCK_HGPR"))
        if hi > 0:
            self.day_high[code] = hi
        with self.lock:
            p = self.acct.positions.get(code)
            if not p:
                return
            if p["overnight"] and p["entry_date"] < self.day and hms >= "090000":
                self.closed.append(self.acct.sell(code, bid, f"{self.day} {hms}", "다음날 시가"))
            elif not p["overnight"] and hms < F.FORCE_HMS:
                self.acct.mark(code, px)
                if self.acct.trail_hit(code):
                    self.closed.append(self.acct.sell(code, bid, f"{self.day} {hms}", "추적손절"))

    def on_index(self, row):
        code, hms = row.get("bstp_cls_code"), row.get("bsop_hour", "")
        mk = next((m for m, c in INDEX_CODES.items() if c == code), None)
        px, op = fnum(row.get("prpr_nmix")), fnum(row.get("oprc_nmix"))
        if not mk or px <= 0:
            return
        with self.dlock:
            self.idx[mk] = dict(now=px, open=op, t=time.time())
            if "090000" <= hms <= "153000":
                self.idx_bars[mk].add(hms, px)

    # ---------------- 스캐너 (REST 스레드) ----------------
    def _call(self, kind, path, tr, params):
        b = self.backoff[kind]
        if not b.allowed():
            return None
        try:
            j = self.rest.get(path, tr, params)
            b.success()
            return j
        except ApiError as e:
            b.failure(str(e))
            return None

    def poll_rankings(self):
        found = {}
        for mk, iscd in INDEX_CODES.items():
            j = self._call("rank", "/uapi/domestic-stock/v1/quotations/volume-rank", "FHPST01710000", dict(
                FID_COND_MRKT_DIV_CODE="J", FID_COND_SCR_DIV_CODE="20171", FID_INPUT_ISCD=iscd, FID_DIV_CLS_CODE="0",
                FID_BLNG_CLS_CODE="1", FID_TRGT_CLS_CODE="111111111", FID_TRGT_EXLS_CLS_CODE=EXCL,
                FID_INPUT_PRICE_1="", FID_INPUT_PRICE_2="", FID_VOL_CNT="", FID_INPUT_DATE_1=""))
            if j is None:
                continue
            rows = rows_of(j, "output")
            self._track(f"거래량급증 {mk}", [(r.get("mksc_shrn_iscd"), r.get("stck_prpr"), r.get("acml_vol")) for r in rows[:10]])
            for r in rows:
                code = r.get("mksc_shrn_iscd", "")
                px, shares = fnum(r.get("stck_prpr")), fnum(r.get("lstn_stcn"))
                found[code] = dict(name=r.get("hts_kor_isnm", ""), market=mk, ctrt=fnum(r.get("prdy_ctrt")),
                                   prev_vol=fnum(r.get("prdy_vol")), cap=px * shares, vol_inrt=fnum(r.get("vol_inrt")))
            # 등락률 순위: 후보 판단에는 거래량 급증 순위 행의 등락률을 쓰고, 이 순위는 갱신 주기 측정·기록용
            j = self._call("rank", "/uapi/domestic-stock/v1/ranking/fluctuation", "FHPST01700000", dict(
                fid_rsfl_rate2="", fid_cond_mrkt_div_code="J", fid_cond_scr_div_code="20170", fid_input_iscd=iscd,
                fid_rank_sort_cls_code="0", fid_input_cnt_1="0", fid_prc_cls_code="0", fid_input_price_1="",
                fid_input_price_2="", fid_vol_cnt="", fid_trgt_cls_code="0", fid_trgt_exls_cls_code=EXCL,
                fid_div_cls_code="0", fid_rsfl_rate1=""))
            if j is not None:
                rows = rows_of(j, "output")
                self._track(f"등락률 {mk}", [(r.get("stck_shrn_iscd"), r.get("stck_prpr"), r.get("acml_vol")) for r in rows[:10]])
        return found

    def _track(self, name, sig):
        dt = self.refresh.seen(name, tuple(sig), time.time())
        if dt:
            log(f"[갱신] {name} 내용 바뀜 (직전 변경 후 {dt:.1f}초)")

    def check_status(self, code):
        """관리종목·거래정지 여부와 시총(억) — 하루 한 번 현재가 조회."""
        if code in self.status:
            return self.status[code]
        j = self._call("price", "/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                       dict(FID_COND_MRKT_DIV_CODE="J", FID_INPUT_ISCD=code))
        if j is None:
            return None
        o = rows_of(j, "output")[0]
        bad = (o.get("iscd_stat_cls_code") in ("51", "58") or o.get("temp_stop_yn") == "Y"
               or o.get("mang_issu_cls_code") == "Y")
        res = (not bad, fnum(o.get("hts_avls")) * 1e8, fnum(o.get("stck_sdpr")))
        self.status[code] = res
        return res

    def poll_program(self, code):
        j = self._call("prog", "/uapi/domestic-stock/v1/quotations/program-trade-by-stock", "FHPPG04650101",
                       dict(FID_COND_MRKT_DIV_CODE="J", FID_INPUT_ISCD=code))
        if j is None:
            return
        rows = [r for r in rows_of(j, "output") if len(r.get("bsop_hour", "")) == 6 and "090000" <= r["bsop_hour"] <= "153000"]
        if not rows:
            return
        with self.dlock:
            s = self.prog.setdefault(code, F.ProgramSeries())
            for r in rows:
                s.add(epoch_of(self.day, r["bsop_hour"]), fnum(r.get("whol_smtn_ntby_qty")))
            self.prog_at[code] = time.time()
            latest = max(r["bsop_hour"] for r in rows)
        self.prog_lags.append(time.time() - epoch_of(self.day, latest))

    def poll_estimate(self, code):
        j = self._call("est", "/uapi/domestic-stock/v1/quotations/investor-trend-estimate", "HHPTJ04160200",
                       dict(MKSC_SHRN_ISCD=code))
        if j is None:
            return
        rows = rows_of(j, "output2", "output", "output1")
        rows = [r for r in rows if r.get("bsop_hour_gb")]
        if not rows:
            return
        r = max(rows, key=lambda x: x["bsop_hour_gb"])
        gb, val = r["bsop_hour_gb"], fnum(r.get("sum_fake_ntby_qty"))
        with self.dlock:
            old = self.est.get(code)
            self.est[code] = (gb, val, time.time())
        if old is None or old[0] != gb:
            log(f"[갱신] 가집계 {code} 구분 {old[0] if old else '-'}→{gb} 합계 {val:,.0f}")

    def prog_view(self, code, t):
        with self.dlock:
            s, at = self.prog.get(code), self.prog_at.get(code, 0)
            if s is None or t - at > PROG_FRESH_SEC:
                return None, None, None if s is None else t - at
            return s.net_since(t), s.latest()[1], t - at

    def est_view(self, code):
        with self.dlock:
            e = self.est.get(code)
        if e is None or hms_now() < EST_VALID_FROM:
            return None, None
        return e[0], e[1]

    def update_watch(self, found):
        """후보 선정 + 웹소켓 감시 목록 교체."""
        t = time.time()
        watched = self.ws.watched()
        with self.lock:
            held = set(self.acct.positions)
            done = set(self.st["signaled"])
        eligible = []
        for code, c in found.items():
            if c["ctrt"] < F.CAND_MIN_CHG or c["cap"] < F.MIN_CAP or code in done:
                continue
            stt = self.check_status(code)
            if stt is None or not stt[0]:
                continue
            c["prev_close"] = stt[2]
            eligible.append(code)
        with self.dlock:
            for code in eligible:
                self.cand[code] = {**self.cand.get(code, {}), **found[code]}
        for code in eligible:                      # 감시 전 종목의 프로그램·가집계
            if code not in watched and t - self.prog_at.get(code, 0) >= ELIGIBLE_PROG_EVERY:
                self.poll_program(code)
            if hms_now() >= EST_VALID_FROM and t - (self.est.get(code) or (0, 0, 0))[2] >= EST_EVERY:
                self.poll_estimate(code)
        # 빼기: 식은 종목, 신호가 끝난 종목(보유 중이면 유지)
        for code in watched:
            if code in held:
                continue
            snap = self.book.snapshot(code)
            chg = snap.get("change_pct") if snap else (self.cand.get(code) or {}).get("ctrt")
            if code in done or (chg is not None and chg < F.CAND_MIN_CHG):
                self.ws.unsubscribe(code)
                log(f"[감시 해제] {code} {'신호 완료' if code in done else f'등락률 {chg:.1f}%'}")
        # 더하기: 수급(프로그램 누적 > 0 또는 가집계 > 0) 만족 후보를 거래증가율 순으로
        for code in held:
            self.ws.subscribe(code)
        watched = self.ws.watched()
        pool = []
        for code in eligible:
            if code in watched:
                continue
            _, cum, _ = self.prog_view(code, t)
            gb, est = self.est_view(code)
            if (cum is not None and cum > 0) or (est is not None and est > 0):
                pool.append((found[code]["vol_inrt"], code))
        for _, code in sorted(pool, reverse=True):
            if len(self.ws.watched()) >= STOCK_SLOTS:
                break
            self.ws.subscribe(code)
            c = self.cand[code]
            log(f"[감시 추가] {code} {c['name']} {c['market']} 등락률 {c['ctrt']:.1f}% 거래증가율 {c['vol_inrt']:.0f}%")

    def morning_resolve(self):
        """전날 '다음날 시가' 대기 중인 사후 계산을 오늘 시가로 확정."""
        keep = []
        for p in self.st.get("pending_sims", []):
            j = self._call("price", "/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                           dict(FID_COND_MRKT_DIV_CODE="J", FID_INPUT_ISCD=p["code"]))
            op = fnum(rows_of(j, "output")[0].get("stck_oprc")) if j else 0
            if op <= 0:
                keep.append(p)
                continue
            append_csv("sim_results.csv", SIM_FIELDS, dict(p, status="closed", exit_date=self.day, exit_time="090000",
                                                           exit=op, reason="다음날 시가", ret_pct=F.net_return(p["entry"], op) * 100))
        with self.lock:
            self.st["pending_sims"] = keep
        return len(keep)

    def scanner_loop(self):
        last = dict(rank=0.0, watch_prog=0.0, morning=0.0)
        while self.running:
            try:
                hms = hms_now()
                if hms >= "090100" and self.st.get("morning_done") != self.day and time.time() - last["morning"] >= 60:
                    last["morning"] = time.time()
                    left = self.morning_resolve()
                    if not left or hms >= "153000":
                        with self.lock:
                            self.st["morning_done"] = self.day
                if "090000" <= hms < "152000":
                    t = time.time()
                    if t - last["rank"] >= RANK_EVERY:
                        last["rank"] = t
                        found = self.poll_rankings()
                        if found:
                            self.update_watch(found)
                    if t - last["watch_prog"] >= WATCH_PROG_EVERY:
                        last["watch_prog"] = t
                        for code in sorted(self.ws.watched()):
                            self.poll_program(code)
                time.sleep(0.2)
            except Exception as e:
                log(f"[스캐너 오류] {type(e).__name__}: {e}")
                time.sleep(2)

    # ---------------- 신호·가상 체결 (메인 스레드) ----------------
    def metrics(self, code, t):
        snap = self.book.snapshot(code)
        with self.dlock:
            c = dict(self.cand.get(code) or {})
        if not snap or not c or t - snap.get("timestamp", 0) > TICK_FRESH_SEC:
            return None
        prog5, cum, age = self.prog_view(code, t)
        gb, est = self.est_view(code)
        with self.dlock:
            ix = dict(self.idx.get(c["market"]) or {})
        if not (F.CHG_MIN <= snap.get("change_pct", 0) <= F.CHG_MAX):     # 전략·그림자 모두 5~12% 필요 → 계산 생략
            return None
        price, vwap = snap.get("price", 0), snap.get("vwap", 0)
        vol3m, normal = self.book.volume_window(code, F.VOL_WIN_SEC), F.normal_3min(c.get("prev_vol", 0))
        return dict(code=code, name=c.get("name"), market=c.get("market"), hms=hms_now(), chg=snap.get("change_pct", 0),
                    vol3m=vol3m, normal3m=normal, vol_ratio=vol3m / normal if normal else None, price=price, vwap=vwap,
                    vwap_gap_pct=(price / vwap - 1) * 100 if vwap else None, prog5=prog5, prog_cum=cum, prog_age=age,
                    cttr=snap.get("strength", 0), idx_now=ix.get("now"), idx_open=ix.get("open"),
                    idx_pct=(ix["now"] / ix["open"] - 1) * 100 if ix.get("open") else None, est_gb=gb, est_sum=est,
                    vol_inrt=c.get("vol_inrt"), cap_eok=c.get("cap", 0) / 1e8, ask1=snap.get("ask_price"),
                    bid1=snap.get("bid_price"), day_high=self.day_high.get(code))

    def evaluate_all(self):
        t = time.time()
        for code in sorted(self.ws.watched()):
            with self.lock:
                if code in self.st["signaled"] and code in self.st["shadowed"]:
                    continue
            m = self.metrics(code, t)
            if m is None:
                continue
            full, base, checks = F.evaluate(m)
            row = dict(m, date=self.day, sig_time=m["hms"])
            if base and code not in self.st["shadowed"]:
                fails = [k for k in F.FLOW_KEYS if not checks[k]]
                append_csv("shadow.csv", SHADOW_FIELDS, dict(row, flow_ok=full, fail_flow="/".join(fails)))
                with self.lock:
                    self.st["shadowed"].append(code)
                    self.st["shadow_open"][code] = dict(sig_time=m["hms"], entry=m["ask1"], name=m["name"])
                log(f"[그림자] {code} {m['name']} 수급 {'통과' if full else '불통과(' + '/'.join(fails) + ')'}")
            if full and code not in self.st["signaled"]:
                self.enter(code, m, row)

    def enter(self, code, m, row):
        with self.lock:
            self.st["signaled"].append(code)
            self.st["signal_open"][code] = dict(sig_time=m["hms"], entry=m["ask1"], name=m["name"])
            prices = self.prices()
            if self.st["halted"]:
                action = "일일 한도로 중단 중"
            elif not self.acct.can_buy():
                action = "자리 없음(2종목 보유)"
            else:
                p = self.acct.buy(code, m["ask1"], prices, f"{self.day} {m['hms']}", self.day,
                                  meta={k: row.get(k) for k in BASE_FIELDS + IND_FIELDS})
                action = f"체결 {p['qty']}주 @ {p['entry']:,.0f}" if p else "수량 0(현금 부족)"
        append_csv("signals.csv", SIGNAL_FIELDS, dict(row, action=action))
        log(f"[신호] {code} {m['name']} 등락률 {m['chg']:.1f}% 체결강도 {m['cttr']:.0f} 프로그램5분 {m['prog5']:,.0f} → {action}")
        self.save_state()

    def prices(self):
        out = {}
        for c in self.acct.positions:
            s = self.book.snapshot(c)
            if s.get("price"):
                out[c] = s["price"]
        return out

    def bid_of(self, code):
        s = self.book.snapshot(code)
        return s.get("bid_price") or s.get("price")

    def flush_closed(self):
        with self.lock:
            items, self.closed = self.closed, []
        for tr in items:
            if not tr:
                continue
            m = tr.pop("meta", {}) or {}
            d, hm = (tr["exit_time"].split(" ") + [""])[:2]
            row = dict(m)
            row.update({k: v for k, v in tr.items() if k not in ("exit_time", "entry_time")})
            row.update(exit_date=d, exit_time=hm, entry_time=tr["entry_time"].split(" ")[-1],
                       pnl_won=tr["qty"] * (tr["exit"] * (1 - F.HALF) - tr["entry"] * (1 + F.HALF)))
            append_csv("trades.csv", TRADE_FIELDS, row)
            log(f"[청산] {tr['code']} {tr['reason']} {tr['entry']:,.0f}→{tr['exit']:,.0f} {tr['ret_pct']:+.2f}%")
        if items:
            self.save_state()

    def check_limit(self):
        with self.lock:
            if self.st["halted"] or not self.acct.positions:
                return
            if self.acct.limit_breached(self.st["day_start_equity"], self.prices()):
                for code in list(self.acct.positions):
                    self.closed.append(self.acct.sell(code, self.bid_of(code), f"{self.day} {hms_now()}", "일일 손실 한도"))
                self.st["halted"] = True
        if self.st["halted"]:
            self.flush_closed()
            telegram(f"일일 손실 한도(-{F.DAILY_LIMIT*100:.0f}%) 도달 — 가상 보유 전량 청산, 오늘 신규 진입 중단")

    def force_1520(self):
        with self.lock:
            for code, p in list(self.acct.positions.items()):
                if p["overnight"]:
                    continue
                s = self.book.snapshot(code)
                px, hi = s.get("price") or p["last"], self.day_high.get(code) or p["peak"]
                if F.decide_1520(px, hi):
                    p["overnight"] = True
                    p["last"] = px
                    log(f"[15:20] {code} 가격 {px:,.0f} 고가 {hi:,.0f} → 다음날 시가 매도로 보유")
                else:
                    self.closed.append(self.acct.sell(code, self.bid_of(code), f"{self.day} {hms_now()}", "15:20 청산"))
            self.st["force_done"] = True
        self.flush_closed()

    # ---------------- 장 마감 후 ----------------
    def minute_bars(self, code, day):
        """당일 1분봉 (HHMMSS, o, h, l, c) — 30개씩 거슬러 조회."""
        out, hour = {}, "153000"
        for _ in range(16):
            j = self._call("price", "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice", "FHKST03010200", dict(
                FID_ETC_CLS_CODE="", FID_COND_MRKT_DIV_CODE="J", FID_INPUT_ISCD=code, FID_INPUT_HOUR_1=hour,
                FID_PW_DATA_INCU_YN="Y"))
            rows = [r for r in rows_of(j or {}, "output2") if r.get("stck_bsop_date") == day]
            if not rows:
                break
            for r in rows:
                out[r["stck_cntg_hour"]] = (r["stck_cntg_hour"], fnum(r["stck_oprc"]), fnum(r["stck_hgpr"]),
                                            fnum(r["stck_lwpr"]), fnum(r["stck_prpr"]))
            first = min(r["stck_cntg_hour"] for r in rows)
            if first <= "090000":
                break
            t = datetime.datetime.strptime(first, "%H%M%S") - datetime.timedelta(minutes=1)
            hour = t.strftime("%H%M%S")
        return sorted(out.values())

    def run_sims(self):
        """오늘 전략 신호·그림자 신호를 분봉으로 같은 규칙 사후 계산."""
        jobs = [("signal", c, v) for c, v in self.st.get("signal_open", {}).items()] + \
               [("shadow", c, v) for c, v in self.st.get("shadow_open", {}).items()]
        cache, pend = {}, []
        for kind, code, v in jobs:
            if code not in cache:
                cache[code] = self.minute_bars(code, self.day)
            r = F.sim_minute(cache[code], v["sig_time"], v["entry"])
            base = dict(kind=kind, date=self.day, code=code, name=v.get("name"), sig_time=v["sig_time"], entry=v["entry"])
            if r["status"] == "overnight":
                pend.append(dict(base, max_pct=r["max_pct"], min_pct=r["min_pct"]))
            elif r["status"] == "closed":
                append_csv("sim_results.csv", SIM_FIELDS, dict(base, status="closed", exit_date=self.day, exit_time=r["exit_hms"],
                                                               exit=r["exit"], reason=r["reason"], ret_pct=F.net_return(v["entry"], r["exit"]) * 100,
                                                               max_pct=r["max_pct"], min_pct=r["min_pct"]))
            else:
                append_csv("sim_results.csv", SIM_FIELDS, dict(base, status="nodata"))
        with self.lock:
            self.st["pending_sims"] = self.st.get("pending_sims", []) + pend
        return len(jobs), len(pend)

    def save_index_minutes(self):
        out_dir = DATA_DIR / "index_min"
        out_dir.mkdir(exist_ok=True)
        notes = []
        for mk, iscd in INDEX_CODES.items():
            with self.dlock:
                ws_rows = {r[0]: r for r in self.idx_bars[mk].rows()}
            j = self._call("price", "/uapi/domestic-stock/v1/quotations/inquire-time-indexchartprice", "FHKUP03500200", dict(
                FID_COND_MRKT_DIV_CODE="U", FID_ETC_CLS_CODE="0", FID_INPUT_ISCD=iscd, FID_INPUT_HOUR_1="60",
                FID_PW_DATA_INCU_YN="Y"))
            rest = {}
            for r in rows_of(j or {}, "output2"):
                h = r.get("stck_cntg_hour", "")
                if r.get("stck_bsop_date") == self.day and h.isdigit() and "090000" <= h <= "153000":
                    rest[h] = (h, fnum(r["bstp_nmix_oprc"]), fnum(r["bstp_nmix_hgpr"]), fnum(r["bstp_nmix_lwpr"]), fnum(r["bstp_nmix_prpr"]))
            both = sorted(set(ws_rows) & set(rest))
            diff = [h for h in both if abs(ws_rows[h][4] - rest[h][4]) > 0.011]
            merged = {**ws_rows, **rest}                       # 겹치는 분은 REST(공식 분봉) 우선
            with open(out_dir / f"{self.day}_{mk}.csv", "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["time", "open", "high", "low", "close", "source"])
                for h in sorted(merged):
                    w.writerow([h, *merged[h][1:], "rest" if h in rest else "ws"])
            notes.append(f"{mk} {len(merged)}분(웹소켓 {len(ws_rows)}, REST {len(rest)}, 겹침 {len(both)} 중 종가 차이 {len(diff)})")
        log("[지수 분봉 저장] " + " / ".join(notes))
        return notes

    def daily_report(self, notes):
        trades = [r for r in read_csv("trades.csv") if r["exit_date"] == self.day]
        with self.lock:
            prices = {c: p["last"] for c, p in self.acct.positions.items()}
            eq = self.acct.equity(prices)
            start = self.st["day_start_equity"]
            opened = {c: dict(p) for c, p in self.acct.positions.items()}
        L = [f"{self.day[:4]}-{self.day[4:6]}-{self.day[6:]} 마감"]
        L.append(f"오늘 거래 {len(trades)}건")
        for r in trades:
            L.append(f"· {r['name']}({r['code']}) {r['entry_time'][:4]} {float(r['entry']):,.0f}→{float(r['exit']):,.0f} "
                     f"{float(r['ret_pct']):+.2f}% [{r['reason']}]")
        for c, p in opened.items():
            L.append(f"· {p['meta'].get('name', c)}({c}) {float(p['entry']):,.0f} 보유 → 다음날 시가 매도 예정")
        L.append(f"가상 자산 {eq:,.0f}원 (오늘 {(eq / start - 1) * 100:+.2f}%, 누적 {(eq / F.START_CASH - 1) * 100:+.2f}%)")
        sims = read_csv("sim_results.csv")
        closed = [r for r in sims if r["status"] == "closed"]

        def stat(rows):
            v = [float(r["ret_pct"]) for r in rows if r.get("ret_pct")]
            return f"{len(v)}건 평균 {F.mean(v):+.2f}%" if v else "0건"
        tod = [r for r in closed if r["date"] == self.day]
        L.append("신호 기준 거래당 성과(비용 차감, 분봉 사후 계산)")
        L.append(f"· 오늘 전략 {stat([r for r in tod if r['kind'] == 'signal'])} / 그림자 {stat([r for r in tod if r['kind'] == 'shadow'])}")
        L.append(f"· 누적 전략 {stat([r for r in closed if r['kind'] == 'signal'])} / 그림자 {stat([r for r in closed if r['kind'] == 'shadow'])}")
        pend = self.st.get("pending_sims", [])
        if pend:
            L.append(f"· 다음날 시가 확정 대기 {len(pend)}건")
        L.append("지수 분봉: " + "; ".join(notes))
        telegram("\n".join(L))
        log("[일일 요약]\n" + "\n".join(L))

    def refresh_summary(self):
        s = self.refresh.summary()
        lags = sorted(self.prog_lags)
        s["프로그램(종목) 지연"] = dict(calls=len(lags), median_sec=lags[len(lags) // 2] if lags else None,
                                  p90_sec=lags[int(len(lags) * .9)] if lags else None)
        with self.dlock:
            s["가집계 구분(종목별 마지막)"] = {c: v[0] for c, v in self.est.items()}
        (DATA_DIR / "refresh").mkdir(exist_ok=True)
        (DATA_DIR / "refresh" / f"{self.day}.json").write_text(json.dumps(s, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
        for k, v in s.items():
            log(f"[갱신 주기 요약] {k}: {v}")

    def after_close(self):
        log("[장 마감] 사후 작업 시작")
        notes = self.save_index_minutes()
        n, p = self.run_sims()
        log(f"[사후 계산] 신호·그림자 {n}건, 다음날 시가 대기 {p}건")
        self.refresh_summary()
        self.daily_report(notes)
        with self.lock:
            self.st["post_close_done"] = self.day
        self.save_state()

    # ---------------- 메인 ----------------
    def run(self):
        with self.lock:
            for code in self.acct.positions:           # 이월 보유 종목은 처음부터 감시
                self.ws.stock_codes.add(code)
        self.ws.start()
        threading.Thread(target=self.scanner_loop, daemon=True).start()
        eq = self.st["day_start_equity"]
        if self.st.get("start_sent") != self.day:          # 재시작 때마다 보내지 않도록 하루 한 번
            telegram(f"시작 — 가상 자산 {eq:,.0f}원(누적 {(eq / F.START_CASH - 1) * 100:+.2f}%), 이월 보유 {len(self.acct.positions)}종목. "
                     f"실전 시세 조회 전용, 주문 없음")
            self.st["start_sent"] = self.day
        log(f"[시작] 가상 자산 {eq:,.0f}원, 이월 보유 {list(self.acct.positions)}")
        last_save = last_eval = 0.0
        gap_state = {}   # 시세 공백 경고 상태 (F.data_gap_alert)
        try:
            while True:
                hms = hms_now()
                if hms >= "153030":
                    break
                if hms >= "090000":
                    gap_msg = F.data_gap_alert(gap_state, self.ws.last_tick_at, time.time(), hms)
                    if gap_msg:
                        log(gap_msg); telegram(gap_msg)
                if not self.ws.connected.is_set():
                    time.sleep(1)
                    continue
                # 15:20~15:30 동시호가에는 체결이 없어 시세 기준 워치독이 헛돌므로 그 전까지만 본다
                if self.ws.is_stale() and "090000" <= hms < "152000":
                    msg = f"웹소켓 {WS_STALE_TIMEOUT_SEC}초 무수신 — 강제 재연결"
                    log(msg); telegram(msg)
                    self.ws.force_reconnect()
                    time.sleep(2)
                    continue
                self.flush_closed()
                if "090000" <= hms < "153000":
                    self.check_limit()
                if hms >= F.FORCE_HMS and not self.st["force_done"]:
                    self.force_1520()
                if F.SIGNAL_START <= hms <= F.SIGNAL_END and time.time() - last_eval >= EVAL_EVERY:
                    last_eval = time.time()
                    self.evaluate_all()
                if time.time() - last_save > 10:
                    self.save_state(); last_save = time.time()
                time.sleep(0.3)
            self.flush_closed()
        finally:
            self.running = False
            self.ws.stop()
            self.save_state()
        if self.st.get("post_close_done") != self.day:
            self.after_close()
        mark_done(self.day, "장 마감 사후 작업 완료")


def mark_done(day, why):
    """run.sh 가 이 파일을 보면 그날은 다시 띄우지 않는다."""
    (STATE_DIR / f"done_{day}").write_text(f"{why} {now().isoformat()}\n", encoding="utf-8")


def trading_day(rest):
    """기존 봇과 같은 순서: KIS 휴장일 API(실전 키 지원) → 안 되면 목록 파일."""
    day = today_str()
    if now().weekday() >= 5:
        return False, "주말"
    opened, reason = C.kis_open_day(rest.key, rest.secret, rest.token(), day)
    if opened is not None:
        return opened, f"KIS 휴장일 API {reason}"
    hol = C.load_holidays()
    if C.trading_day_status(now(), hol, []) == "holiday":
        return False, "휴장일 목록"
    return True, f"휴장일 API 사용 불가({reason}) — 목록상 개장일"


def smoke(key, secret, seconds):
    """조회만 하는 점검: 순위 1회·후보 프로그램 조회·웹소켓 연결. 상태·데이터 파일은 쓰지 않는다."""
    global STATE_FILE, DATA_DIR
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="flow_smoke_"))
    STATE_FILE, DATA_DIR = tmp / "state.json", tmp
    b = Bot(key, secret)
    found = b.poll_rankings()
    elig = [c for c, v in found.items() if v["ctrt"] >= 3 and v["cap"] >= F.MIN_CAP]
    print(f"순위 종목 {len(found)}, 등락률 3%↑·시총 1000억↑ {len(elig)}")
    for code in elig[:3]:
        print(code, found[code]["name"], "상태", b.check_status(code))
        b.poll_program(code)
        s = b.prog.get(code)
        print("  프로그램 점", 0 if s is None else len(s.t))
        b.poll_estimate(code)
    b.ws.start()
    time.sleep(seconds)
    print("웹소켓 연결", b.ws.connected.is_set(), "등록 응답", len(b.ws.sub_status), set(b.ws.sub_status.values()))
    b.ws.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", type=int, default=0, help="N초 동안 조회 전용 점검 후 종료")
    a = ap.parse_args()
    key, secret = os.environ.get("KIS_REAL_APP_KEY"), os.environ.get("KIS_REAL_APP_SECRET")
    if not key or not secret:
        raise SystemExit("KIS_REAL_APP_KEY / KIS_REAL_APP_SECRET 환경변수가 필요합니다(.env).")
    STATE_DIR.mkdir(exist_ok=True); DATA_DIR.mkdir(exist_ok=True)
    if a.smoke:
        return smoke(key, secret, a.smoke)
    rest = KisRest(key, secret)
    ok, why = trading_day(rest)
    if not ok:
        log(f"[휴장] {today_str()} {why} — 오늘은 실행하지 않습니다.")
        mark_done(today_str(), f"휴장 {why}")
        return
    log(f"[개장일] {why}")
    if hms_now() >= "153030":
        log("장 종료 후 시작 — 사후 작업만 확인합니다.")
    Bot(key, secret).run()


if __name__ == "__main__":
    main()
