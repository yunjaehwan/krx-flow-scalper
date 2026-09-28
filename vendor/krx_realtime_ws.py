# -*- coding: utf-8 -*-
"""
KIS 국내주식 실시간 WebSocket 데이터 모듈
==========================================
- H0UNCNT0: KRX+NXT 통합 실시간 체결
- 자동 재접속 (지수 백오프)
- 종목별 최근 체결량/가격 버퍼 제공 (최근 N초 거래량 가속도, 가격 변화 계산용)

필요 패키지:
    pip install websocket-client requests

참고: 한 연결이 동시에 구독할 수 있는 종목 수에 상한이 있을 수 있다는
얘기가 있으나 정확한 현재 값은 공식 문서로 확인하지 못했습니다.
후보 종목 수(PREFILTER_TOP_N, 실행 파일 쪽 설정)를 보수적으로 잡고,
실제로 몇 종목까지 정상 수신되는지 로그로 확인하는 걸 권장합니다.
"""
import csv
import json
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

import requests

try:
    import websocket
except ImportError:
    websocket = None


KST = timezone(timedelta(hours=9))
MOCK_WS_URL = "ws://ops.koreainvestment.com:31000"
REAL_WS_URL = "ws://ops.koreainvestment.com:21000"
WS_STALE_TIMEOUT_SEC = 60  # 이 시간 동안 구독 종목 전체에서 시세 데이터가 한 건도 없으면 "좀비 연결"로 보고 강제 재연결
# (서버 PINGPONG·구독 응답은 세지 않음: 2026-09-28 구독 없이 PINGPONG만 오는 연결을 45분간 못 잡았음)

TRADE_TR_ID = "H0UNCNT0"  # KRX+NXT 통합 실시간 체결

# H0UNCNT0 공식 필드 순서 (체결 데이터가 파이프/캐럿으로 구분되어 옴)
PRICE_FIELDS = [
    "MKSC_SHRN_ISCD", "STCK_CNTG_HOUR", "STCK_PRPR", "PRDY_VRSS_SIGN",
    "PRDY_VRSS", "PRDY_CTRT", "WGHN_AVRG_STCK_PRC", "STCK_OPRC",
    "STCK_HGPR", "STCK_LWPR", "ASKP1", "BIDP1", "CNTG_VOL", "ACML_VOL",
    "ACML_TR_PBMN", "SELN_CNTG_CSNU", "SHNU_CNTG_CSNU", "NTBY_CNTG_CSNU",
    "CTTR", "SELN_CNTG_SMTN", "SHNU_CNTG_SMTN", "CNTG_CLS_CODE", "SHNU_RATE",
    "PRDY_VOL_VRSS_ACML_VOL_RATE", "OPRC_HOUR", "OPRC_VRSS_PRPR_SIGN",
    "OPRC_VRSS_PRPR", "HGPR_HOUR", "HGPR_VRSS_PRPR_SIGN", "HGPR_VRSS_PRPR",
    "LWPR_HOUR", "LWPR_VRSS_PRPR_SIGN", "LWPR_VRSS_PRPR", "BSOP_DATE",
    "NEW_MKOP_CLS_CODE", "TRHT_YN", "ASKP_RSQN1", "BIDP_RSQN1",
    "TOTAL_ASKP_RSQN", "TOTAL_BIDP_RSQN", "VOL_TNRT",
    "PRDY_SMNS_HOUR_ACML_VOL", "PRDY_SMNS_HOUR_ACML_VOL_RATE",
    "HOUR_CLS_CODE", "MRKT_TRTM_CLS_CODE", "VI_STND_PRC",
]


def split_records(values, count, n_fields):
    """'^'로 나눈 값 목록을 레코드 단위로 자른다. 레코드 길이는 (값 개수 ÷ 레코드 수)로 정한다.
    2026-09-28 모의 서버 H0UNCNT0 레코드는 문서(46개)보다 1개 많은 47개였고, 46개씩 자르면
    두 번째 레코드부터 한 칸씩 밀려 쓰레기 종목코드가 됐다. 나누어떨어지지 않거나 레코드 수 칸이
    이상하면 n_fields 로 자른다. 앞의 n_fields 개만 이름이 붙고 추가 필드는 버려진다."""
    if count > 0 and len(values) % count == 0 and len(values) // count >= n_fields:
        stride = len(values) // count
    else:
        stride = n_fields
        count = len(values) // stride if count <= 0 else min(count, len(values) // stride)
    return [values[i * stride:(i + 1) * stride] for i in range(count)]


EXTRA_TICK_FIELDS = ("trade_time", "trade_side_code", "strength", "cum_sell_volume", "cum_buy_volume",
                     "sell_count", "buy_count", "ask_qty1", "bid_qty1", "total_ask_qty", "total_bid_qty")

# 진단용: 장 시작 후 이 구간(KST)에 받은 실시간 데이터 메시지를 원본 그대로 logs/ws_raw_YYYYMMDD.log 에 남긴다.
RAW_LOG_START, RAW_LOG_END = "09:00:00", "09:10:00"
RAW_LOG_DIR = "./logs"


def _f(v, default=0.0):
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return default


def get_approval_key(app_key: str, app_secret: str, is_mock: bool) -> str:
    base = ("https://openapivts.koreainvestment.com:29443" if is_mock
            else "https://openapi.koreainvestment.com:9443")
    r = requests.post(
        f"{base}/oauth2/Approval",
        headers={"content-type": "application/json"},
        json={"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("approval_key"):
        raise RuntimeError(f"WebSocket approval key 발급 실패: {data}")
    return data["approval_key"]


class TickArchiver:
    """종목별 실시간 틱을 날짜별 CSV(krx_tick_archive/<YYYYMMDD>/<종목코드>.csv)로 축적한다.
    record()는 메모리 버퍼에 쌓기만 하고(락만 짧게 잡음), 실제 디스크 쓰기는 flush()가
    flush_interval_sec 이상 지났을 때만 수행한다 — WS 수신 스레드가 디스크 I/O로
    지연되지 않도록 record()와 flush()의 파일 쓰기 구간을 분리했다."""

    # 뒤쪽 열은 수급 전략 검증용 (2026-09-27 추가): 체결시각, 체결구분(1 매수·5 매도), 체결강도,
    # 누적 매도/매수 체결량, 매도/매수 체결 건수, 최우선·총 호가잔량
    FIELDS = ["timestamp", "price", "change_pct", "tick_volume",
              "cumulative_volume", "cumulative_trade_value", "vwap",
              "ask_price", "bid_price",
              "trade_time", "trade_side_code", "strength",
              "cum_sell_volume", "cum_buy_volume", "sell_count", "buy_count",
              "ask_qty1", "bid_qty1", "total_ask_qty", "total_bid_qty"]

    def __init__(self, base_dir="./krx_tick_archive", flush_interval_sec=5):
        self.base_dir = base_dir
        self.flush_interval_sec = flush_interval_sec
        self.lock = threading.RLock()
        self.buffers = defaultdict(list)
        self.last_flush_at = 0.0

    def _path_for(self, day_dir, ticker):
        """같은 날 예전 형식(열 개수가 다른) 파일이 이미 있으면 섞이지 않게 .v2.csv 로 따로 쓴다."""
        path = os.path.join(day_dir, f"{ticker}.csv")
        if os.path.exists(path):
            with open(path, encoding="utf-8-sig") as f:
                header = f.readline().strip().split(",")
            if header != self.FIELDS:
                return os.path.join(day_dir, f"{ticker}.v2.csv")
        return path

    def record(self, ticker, row):
        with self.lock:
            self.buffers[ticker].append([row.get(f, "") for f in self.FIELDS])

    def flush(self, force=False):
        now = time.time()
        with self.lock:
            if not force and now - self.last_flush_at < self.flush_interval_sec:
                return
            self.last_flush_at = now
            if not self.buffers:
                return
            pending = self.buffers
            self.buffers = defaultdict(list)

        # 실제 파일 I/O는 락 밖에서 수행 (record()가 이 동안 블록되지 않도록)
        day_dir = os.path.join(self.base_dir, datetime.now(KST).strftime("%Y%m%d"))
        os.makedirs(day_dir, exist_ok=True)
        for ticker, rows in pending.items():
            if not rows:
                continue
            path = self._path_for(day_dir, ticker)
            is_new = not os.path.exists(path)
            with open(path, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                if is_new:
                    writer.writerow(self.FIELDS)
                writer.writerows(rows)


class RealtimeBook:
    """전략에서 읽는 종목별 실시간 상태. 스레드 세이프.

    now_fn: 시각을 얻는 함수(기본 time.time). 백테스트 리플레이가 실제 현재 시각이 아니라
    재생 중인 과거 시각을 기준으로 volume_window()/price_change_window()가 동작하게 하려면
    이 자리에 리플레이용 시계 함수를 주입한다 (RealtimeBook(now_fn=clock.time))."""

    def __init__(self, max_age_sec=300, archiver=None, now_fn=time.time):  # 최대 윈도우(180초)보다 여유 있게
        self.lock = threading.RLock()
        self.data = {}
        self.ticks = defaultdict(deque)
        self.max_age_sec = max_age_sec
        self.archiver = archiver
        self.now_fn = now_fn

    def apply_tick(self, code, parsed, trade_time=""):
        """이미 파싱된 값(parsed dict: price/change_pct/tick_volume/cumulative_volume/
        cumulative_trade_value/vwap/ask_price/bid_price)으로 book 상태를 갱신한다.
        update_trade()(실시간 WS 경로)와 백테스트 리플레이가 이 메서드를 공유한다."""
        now = self.now_fn()
        price = parsed.get("price", 0.0)
        tick_vol = parsed.get("tick_volume", 0.0)
        acml_vol = parsed.get("cumulative_volume", 0.0)

        with self.lock:
            old = self.data.get(code, {})
            row_out = {
                **old,
                "ticker": code,
                "timestamp": now,
                "trade_time": trade_time,
                "price": price,
                "change_pct": parsed.get("change_pct", 0.0),
                "tick_volume": tick_vol,
                "cumulative_volume": acml_vol,
                "cumulative_trade_value": parsed.get("cumulative_trade_value", 0.0),
                "vwap": parsed.get("vwap", 0.0),
                "ask_price": parsed.get("ask_price", 0.0),
                "bid_price": parsed.get("bid_price", 0.0),
            }
            for k in EXTRA_TICK_FIELDS:            # 수급 필드(있을 때만) — 전략 판단에는 쓰지 않고 아카이브용
                if k in parsed:
                    row_out[k] = parsed[k]
            self.data[code] = row_out
            self.ticks[code].append((now, price, tick_vol, acml_vol))
            cutoff = now - self.max_age_sec
            dq = self.ticks[code]
            while dq and dq[0][0] < cutoff:
                dq.popleft()

        if self.archiver:
            self.archiver.record(code, row_out)
        return row_out

    def update_trade(self, row):
        code = row.get("MKSC_SHRN_ISCD", "")
        if not code:
            return
        parsed = {
            "price": _f(row.get("STCK_PRPR")),
            "change_pct": _f(row.get("PRDY_CTRT")),
            "tick_volume": _f(row.get("CNTG_VOL")),
            "cumulative_volume": _f(row.get("ACML_VOL")),
            "cumulative_trade_value": _f(row.get("ACML_TR_PBMN")),
            "vwap": _f(row.get("WGHN_AVRG_STCK_PRC")),
            "ask_price": _f(row.get("ASKP1")),
            "bid_price": _f(row.get("BIDP1")),
            "trade_time": row.get("STCK_CNTG_HOUR", ""),
            "trade_side_code": row.get("CNTG_CLS_CODE", ""),
            "strength": _f(row.get("CTTR")),
            "cum_sell_volume": _f(row.get("SELN_CNTG_SMTN")),
            "cum_buy_volume": _f(row.get("SHNU_CNTG_SMTN")),
            "sell_count": _f(row.get("SELN_CNTG_CSNU")),
            "buy_count": _f(row.get("SHNU_CNTG_CSNU")),
            "ask_qty1": _f(row.get("ASKP_RSQN1")),
            "bid_qty1": _f(row.get("BIDP_RSQN1")),
            "total_ask_qty": _f(row.get("TOTAL_ASKP_RSQN")),
            "total_bid_qty": _f(row.get("TOTAL_BIDP_RSQN")),
        }
        self.apply_tick(code, parsed, trade_time=row.get("STCK_CNTG_HOUR", ""))

    def snapshot(self, ticker):
        with self.lock:
            return dict(self.data.get(ticker, {}))

    def volume_window(self, ticker, seconds):
        """최근 seconds초 거래량 = (마지막 틱 누적거래량) - (창 시작 시점 누적거래량).
        예전에는 창 안의 첫 틱과 마지막 틱의 차이만 봐서, 창 안 틱이 드문드문하면(수신 누락·장 초반 혼잡)
        창 시작~첫 틱 사이 거래량이 빠지고, 틱이 1개뿐이면 그 틱 한 건의 체결량만 돌려줘서 거래량을
        크게 과소 계산했다(2026-09-23 장 초반 실제 거래량의 3~26%).
        이제는 창 시작 시점의 누적거래량을 창 앞뒤 두 틱 사이에서 선형 보간해서 구한다.
        창 앞 틱이 없으면(시작 직후) 창 안 첫 틱의 체결량을 더한다."""
        cutoff = self.now_fn() - seconds
        with self.lock:
            ticks = list(self.ticks.get(ticker, ()))
        recent = [x for x in ticks if x[0] >= cutoff]
        if not recent:
            return 0.0
        before = [x for x in ticks if x[0] < cutoff]
        first, last = recent[0], recent[-1]
        if before and before[-1][3] > 0 and first[3] > 0:
            b = before[-1]
            span = first[0] - b[0]
            frac = (cutoff - b[0]) / span if span > 0 else 1.0
            cum_at_cutoff = b[3] + (first[3] - b[3]) * frac
            return max(0.0, last[3] - cum_at_cutoff)
        if first[3] > 0:
            return max(0.0, last[3] - first[3] + first[2])
        return sum(x[2] for x in recent)

    def price_change_window(self, ticker, seconds):
        cutoff = self.now_fn() - seconds
        with self.lock:
            ticks = list(self.ticks.get(ticker, ()))
        recent = [x for x in ticks if x[0] >= cutoff]
        if len(recent) < 2:
            return 0.0
        first, last = recent[0][1], recent[-1][1]
        if first <= 0:
            return 0.0
        return (last - first) / first * 100.0


class KISRealtimeWS:
    def __init__(self, app_key, app_secret, is_mock=True, book=None,
                 reconnect_min=2, reconnect_max=30):
        if websocket is None:
            raise RuntimeError("websocket-client 패키지가 필요합니다: pip install websocket-client")
        self.app_key = app_key
        self.app_secret = app_secret
        self.is_mock = is_mock
        self.book = book or RealtimeBook()
        self.reconnect_min = reconnect_min
        self.reconnect_max = reconnect_max
        self.stock_codes = set()
        self.running = False
        self.ws = None
        self.thread = None
        self.connected = threading.Event()
        self.last_message_at = 0.0   # PINGPONG 포함 모든 메시지
        self.last_data_at = 0.0      # 시세 데이터(0|, 1|)만 — 워치독 기준
        self._raw_fp = None
        self._raw_flushed_at = 0.0

    def add_symbols(self, symbols):
        self.stock_codes.update(str(x).zfill(6) for x in symbols)

    def _message(self, tr_id, tr_key, tr_type="1", approval_key=None):
        return json.dumps({
            "header": {
                "approval_key": approval_key, "custtype": "P",
                "tr_type": tr_type, "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
        })

    def _on_open(self, ws):
        self.connected.set()
        # 재연결 직후 첫 틱이 아직 안 왔을 때 바로 stale 판정 나는 것 방지(유예)
        self.last_message_at = self.last_data_at = time.time()
        try:
            self._subscribe_all(ws)
        except Exception as e:
            # 예전에는 여기서 예외가 나면 구독 없이 연결만 살아있는 상태로 남았다(2026-09-28 승인키
            # 발급 timeout). 소켓을 닫아 _run()의 재연결 루프가 백오프 후 처음부터 다시 하게 한다.
            print(f"[WS] 구독 실패 → 소켓을 닫고 재연결합니다: {e}")
            self.connected.clear()
            try:
                ws.close()
            except Exception:
                pass

    def _subscribe_all(self, ws):
        approval = get_approval_key(self.app_key, self.app_secret, self.is_mock)
        for code in sorted(self.stock_codes):
            ws.send(self._message(TRADE_TR_ID, code, "1", approval))
            time.sleep(0.03)
        print(f"[WS] 연결/체결 구독 완료: {len(self.stock_codes)}종목")

    def _parse_trade(self, raw):
        """'0|H0UNCNT0|003|레코드1^레코드2^레코드3' 형식. 세 번째 칸이 레코드 수이고, 레코드마다
        PRICE_FIELDS(46개) 값이 '^'로 이어 붙어 온다. 예전 코드는 첫 레코드만 읽고 나머지를 버려서
        틱 아카이브 체결량이 실제의 약 60%만 남았다 → 모든 레코드를 순서대로 반영한다."""
        parts = raw.split("|")
        if len(parts) < 4 or parts[1] != TRADE_TR_ID:
            return 0
        values = parts[3].split("^")
        try:
            count = int(parts[2])
        except ValueError:
            count = 0
        records = split_records(values, count, len(PRICE_FIELDS))
        for rec in records:
            self.book.update_trade(dict(zip(PRICE_FIELDS, rec)))
        return len(records)

    def _raw_log(self, message):
        """RAW_LOG_START~RAW_LOG_END(KST) 동안 받은 실시간 데이터 메시지를 원본 그대로 저장(진단용).
        시세 데이터만 기록한다(구독 요청·응답 JSON은 쓰지 않음)."""
        now = datetime.now(KST)
        hms = now.strftime("%H:%M:%S")
        if not (RAW_LOG_START <= hms < RAW_LOG_END):
            if self._raw_fp:
                self._raw_fp.close()
                self._raw_fp = None
            return
        if self._raw_fp is None:
            os.makedirs(RAW_LOG_DIR, exist_ok=True)
            self._raw_fp = open(os.path.join(RAW_LOG_DIR, f"ws_raw_{now:%Y%m%d}.log"), "a", encoding="utf-8")
        self._raw_fp.write(f"{time.time():.6f}\t{message}\n")
        if time.time() - self._raw_flushed_at > 1.0:
            self._raw_fp.flush()
            self._raw_flushed_at = time.time()

    def _on_message(self, ws, message):
        self.last_message_at = time.time()
        if not message:
            return
        if message[:2] in ("0|", "1|"):
            self.last_data_at = time.time()
            try:
                self._raw_log(message)
            except Exception as e:                # 진단 기록 실패가 수신을 막으면 안 됨
                print(f"[WS 진단로그 실패] {e}")
        if message.startswith("0|"):
            self._parse_trade(message)
            return
        if message.startswith("1|"):
            return
        try:
            obj = json.loads(message)
            tr_id = obj.get("header", {}).get("tr_id")
            if tr_id == "PINGPONG":
                ws.send(message)
            else:
                msg = obj.get("body", {}).get("msg1", "")
                if msg:
                    print(f"[WS] {tr_id}: {msg}")
        except Exception:
            pass

    def _on_error(self, ws, error):
        print(f"[WS] 오류: {error}")

    def _on_close(self, ws, code, msg):
        self.connected.clear()
        print(f"[WS] 종료: {code} {msg}")

    def is_stale(self):
        """WS_STALE_TIMEOUT_SEC 동안 구독 종목 전체에서 시세 데이터가 한 건도 없었는지
        (TCP는 살아있고 PINGPONG도 오지만 데이터가 안 오는 '좀비 연결')."""
        return self.last_data_at > 0 and time.time() - self.last_data_at > WS_STALE_TIMEOUT_SEC

    def force_reconnect(self):
        """현재 소켓만 닫는다(running은 유지) -> _run()의 기존 재연결 루프가
        그대로 백오프+재구독을 수행한다. 새 재연결 로직을 따로 만들지 않고
        이미 검증된 on_close 경로를 재사용하기 위함."""
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def _next_delay(self, delay, connected_at, closed_cleanly):
        """run_forever()가 반환된 뒤 다음 재연결까지 기다릴 시간을 계산한다.
        연결이 reconnect_max 이상 유지되다가 정상적으로(on_close) 끊긴 거라면
        '오래 떠 있다가 어쩌다 한 번 끊긴 것'으로 보고 백오프를 초기화한다.
        그렇지 않으면(반복적으로 빨리 끊기는 상황) delay를 그대로 유지해서
        바깥의 time.sleep(delay); delay *= 2 가 실제로 누적되게 한다.
        (예전 코드는 정상 종료마다 무조건 초기화해서 사실상 백오프가 안 늘어났었음)"""
        if closed_cleanly and time.time() - connected_at >= self.reconnect_max:
            return self.reconnect_min
        return delay

    def _run(self):
        delay = self.reconnect_min
        while self.running:
            connected_at = time.time()
            closed_cleanly = False
            try:
                url = MOCK_WS_URL if self.is_mock else REAL_WS_URL
                self.ws = websocket.WebSocketApp(
                    url, on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_error, on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10, ping_payload="KIS")
                closed_cleanly = True
            except Exception as e:
                print(f"[WS] 재접속 예외: {e}")
            delay = self._next_delay(delay, connected_at, closed_cleanly)
            if self.running:
                time.sleep(delay)
                delay = min(self.reconnect_max, delay * 2)

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self.connected.clear()
