# -*- coding: utf-8 -*-
"""수급 단타 가상매매 — 순수 로직(네트워크 없음, 테스트 대상).

- 신호 판정(evaluate): 전략 신호 = 모든 조건, 그림자 신호 = 수급 조건(프로그램·체결강도)만 뺀 나머지
- 가상 계좌(VirtualAccount): 매수는 매도1호가, 매도는 매수1호가, 왕복 비용 0.45%(매수·매도 반씩)
- 분봉 사후 시뮬레이션(sim_minute): 그림자·신호 기준 거래당 성과를 장 마감 후 같은 규칙으로 계산
- 프로그램 5분 순매수(ProgramSeries), 지수 1분봉 만들기(MinuteBars), API 갱신 주기 측정(RefreshTracker)
"""
import bisect
import math
import statistics

# ---------------- 규칙 (사용자 설계 그대로) ----------------
COST_RT = 0.0045              # 왕복 비용
HALF = COST_RT / 2
TRAIL = 0.03                  # 고점 대비 -3% 추적손절
NEAR_HIGH = 0.02              # 15:20 가격이 당일 고가 -2% 이내면 다음날 시가 매도
POS_FRAC = 0.25               # 한 종목 자산 25%
MAX_POS = 2
DAILY_LIMIT = 0.02            # 일일 -2%
START_CASH = 10_000_000
CHG_MIN, CHG_MAX = 5.0, 12.0
SURGE = 2.0
VOL_WIN_SEC = 180
SESSION_SEC = 23400
VWAP_GAP_MAX = 0.02
CTTR_MIN = 120.0
PROG_WIN_SEC = 300
SIGNAL_START, SIGNAL_END = "090000", "143000"
FORCE_HMS = "152000"
CAND_MIN_CHG = 3.0            # 후보·감시 유지 기준
MIN_CAP = 100_000_000_000     # 시총 1000억
FLOW_KEYS = ("프로그램5분", "프로그램누적", "체결강도")


def evaluate(m):
    """m: 신호 시점 지표 dict. 반환 (전략신호 여부, 그림자신호 여부, 조건별 결과)."""
    vwap, price = m.get("vwap") or 0, m.get("price") or 0
    idx_now, idx_open = m.get("idx_now"), m.get("idx_open")
    checks = {
        "등락률": CHG_MIN <= m["chg"] <= CHG_MAX,
        "거래량": m["normal3m"] > 0 and m["vol3m"] >= m["normal3m"] * SURGE,
        "VWAP위": vwap > 0 and price >= vwap,
        "VWAP괴리": vwap > 0 and price <= vwap * (1 + VWAP_GAP_MAX),
        "지수": bool(idx_now and idx_open and idx_now > idx_open),
        "시간": SIGNAL_START <= m["hms"] <= SIGNAL_END,
        "프로그램5분": m.get("prog5") is not None and m["prog5"] > 0,
        "프로그램누적": m.get("prog_cum") is not None and m["prog_cum"] > 0,
        "체결강도": (m.get("cttr") or 0) >= CTTR_MIN,
    }
    base_ok = all(v for k, v in checks.items() if k not in FLOW_KEYS)
    return base_ok and all(checks[k] for k in FLOW_KEYS), base_ok, checks


def normal_3min(prev_vol):
    return prev_vol / SESSION_SEC * VOL_WIN_SEC


def net_return(entry, exit_):
    """비용 차감 수익률(소수). 매수에 반, 매도에 반."""
    return (exit_ * (1 - HALF)) / (entry * (1 + HALF)) - 1


# ---------------- 가상 계좌 ----------------
class VirtualAccount:
    """positions[code] = dict(qty, entry, entry_time, entry_date, peak, trough, last, overnight, meta)."""

    def __init__(self, cash=START_CASH, positions=None):
        self.cash = float(cash)
        self.positions = positions or {}

    def equity(self, prices):
        return self.cash + sum(p["qty"] * (prices.get(c) or p["last"]) for c, p in self.positions.items())

    def can_buy(self):
        return len(self.positions) < MAX_POS

    def buy(self, code, ask, prices, when, date, meta=None):
        """자산(현재가 평가)의 25%, 현금이 모자라면 남은 현금만큼. 정수 주. 못 사면 None."""
        if not self.can_buy() or code in self.positions or not ask or ask <= 0:
            return None
        budget = min(POS_FRAC * self.equity(prices), self.cash)
        qty = int(budget // (ask * (1 + HALF)))
        if qty < 1:
            return None
        self.cash -= qty * ask * (1 + HALF)
        self.positions[code] = dict(qty=qty, entry=ask, entry_time=when, entry_date=date, peak=ask, trough=ask,
                                    last=ask, overnight=False, meta=meta or {})
        return self.positions[code]

    def mark(self, code, price):
        """틱 반영: 보유 중 최고·최저 갱신."""
        p = self.positions.get(code)
        if p and price and price > 0:
            p["last"] = price
            p["peak"] = max(p["peak"], price)
            p["trough"] = min(p["trough"], price)
        return p

    def trail_hit(self, code):
        p = self.positions.get(code)
        return bool(p and p["last"] <= p["peak"] * (1 - TRAIL))

    def sell(self, code, bid, when, reason):
        p = self.positions.pop(code, None)
        if p is None:
            return None
        bid = bid if bid and bid > 0 else p["last"]
        self.cash += p["qty"] * bid * (1 - HALF)
        return dict(code=code, qty=p["qty"], entry=p["entry"], entry_time=p["entry_time"], entry_date=p["entry_date"],
                    exit=bid, exit_time=when, reason=reason, ret_pct=net_return(p["entry"], bid) * 100,
                    max_px=p["peak"], min_px=p["trough"],
                    max_pct=(p["peak"] / p["entry"] - 1) * 100, min_pct=(p["trough"] / p["entry"] - 1) * 100,
                    meta=p["meta"])

    def limit_breached(self, day_start_equity, prices):
        return self.equity(prices) <= day_start_equity * (1 - DAILY_LIMIT)

    def to_json(self):
        return dict(cash=self.cash, positions=self.positions)


def decide_1520(price_1520, day_high):
    """True면 다음날 시가 매도(보유 유지), False면 지금 매도."""
    return bool(price_1520 and day_high and price_1520 >= day_high * (1 - NEAR_HIGH))


# ---------------- 분봉 사후 시뮬레이션 ----------------
def sim_minute(bars, sig_hms, entry):
    """bars: [(HHMMSS, o, h, l, c)] 당일 정규장 1분봉(시각 오름차순). 신호가 난 분봉 다음 분봉부터 추적.
    분봉 안의 가격 순서는 흔히 쓰는 가정을 따른다: 양봉(종가≥시가)은 시가→저가→고가→종가, 음봉은 시가→고가→저가→종가.
    시가가 이미 손절선 아래면(갭) 시가에, 그 밖에는 손절선 가격에 판다.
    반환 dict(status='closed'|'overnight'|'nodata', exit, exit_hms, reason, max_pct, min_pct)."""
    bars = sorted(b for b in bars if b[0] < "153100")
    after = [b for b in bars if b[0][:4] > sig_hms[:4]]
    pre = [b for b in bars if b[0] < FORCE_HMS]
    if not pre or not entry:
        return dict(status="nodata")
    peak, hi, lo = entry, entry, entry
    for t, o, h, l, c in after:
        if t >= FORCE_HMS:
            break
        path = (o, l, h, c) if c >= o else (o, h, l, c)
        for k, px in enumerate(path):
            stop = peak * (1 - TRAIL)
            if px <= stop:
                ex = px if k == 0 else stop
                lo = min(lo, ex)
                return dict(status="closed", exit=ex, exit_hms=t, reason="추적손절(갭)" if k == 0 else "추적손절",
                            max_pct=(hi / entry - 1) * 100, min_pct=(lo / entry - 1) * 100)
            peak, hi, lo = max(peak, px), max(hi, px), min(lo, px)
    p1520 = pre[-1][4]
    day_high = max(b[2] for b in pre)
    res = dict(max_pct=(hi / entry - 1) * 100, min_pct=(lo / entry - 1) * 100)
    if decide_1520(p1520, day_high):
        return dict(res, status="overnight", exit=None, exit_hms=None, reason="다음날 시가")
    return dict(res, status="closed", exit=p1520, exit_hms=pre[-1][0], reason="15:20 청산")


# ---------------- 프로그램 순매수 시계열 ----------------
class ProgramSeries:
    """(epoch초, 당일 누적 프로그램 순매수 수량) 점들. 5분 순매수 = 지금 누적 - 5분 전 시점 누적."""

    def __init__(self):
        self.t, self.v = [], []

    def add(self, ts, cum):
        i = bisect.bisect_left(self.t, ts)
        if i < len(self.t) and self.t[i] == ts:
            self.v[i] = cum
        else:
            self.t.insert(i, ts); self.v.insert(i, cum)

    def latest(self):
        return (self.t[-1], self.v[-1]) if self.t else (None, None)

    def net_since(self, now, sec=PROG_WIN_SEC):
        """5분 전 시점(또는 그 이전)의 점이 없으면 None(이력 부족)."""
        if not self.t:
            return None
        i = bisect.bisect_right(self.t, now - sec) - 1
        if i < 0:
            return None
        return self.v[-1] - self.v[i]


# ---------------- 지수 1분봉 ----------------
class MinuteBars:
    def __init__(self):
        self.bars = {}      # "HHMM" -> [o, h, l, c]

    def add(self, hms, px):
        if not px or px <= 0 or len(hms) < 4:
            return
        k = hms[:4]
        b = self.bars.get(k)
        if b is None:
            self.bars[k] = [px, px, px, px]
        else:
            b[1] = max(b[1], px); b[2] = min(b[2], px); b[3] = px

    def rows(self):
        return [(k + "00", *v) for k, v in sorted(self.bars.items())]


# ---------------- API 갱신 주기 측정 ----------------
class RefreshTracker:
    """같은 API를 반복 조회하면서 응답 내용이 바뀐 시점 간격을 기록(갱신 주기 추정용)."""

    def __init__(self):
        self.last_sig, self.last_change, self.intervals, self.calls = {}, {}, {}, {}

    def seen(self, name, sig, now):
        """바뀌었으면 직전 변경 후 경과초(첫 관측은 0), 안 바뀌었으면 None."""
        self.calls[name] = self.calls.get(name, 0) + 1
        if self.last_sig.get(name) == sig:
            return None
        self.last_sig[name] = sig
        prev = self.last_change.get(name)
        self.last_change[name] = now
        if prev is None:
            return 0.0
        dt = now - prev
        self.intervals.setdefault(name, []).append(dt)
        return dt

    def summary(self):
        out = {}
        for name, n in self.calls.items():
            iv = self.intervals.get(name, [])
            out[name] = dict(calls=n, changes=len(iv), median_sec=statistics.median(iv) if iv else None,
                             min_sec=min(iv) if iv else None)
        return out


def mean(xs):
    xs = [x for x in xs if x is not None and not (isinstance(x, float) and math.isnan(x))]
    return sum(xs) / len(xs) if xs else None
