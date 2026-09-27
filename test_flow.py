# -*- coding: utf-8 -*-
"""네트워크 없이 도는 단위 테스트. 실행: .venv/bin/python -m unittest test_flow -v
실제 state/·data/ 는 건드리지 않는다(임시 폴더로 바꿔서 실행)."""
import os
import re
import tempfile
import time
import unittest
from pathlib import Path

for k in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
    os.environ.pop(k, None)                       # 테스트 중 텔레그램 전송 금지

import flow_core as F
import krx_flow_scalper as S

HERE = Path(__file__).resolve().parent


def metrics(**kw):
    m = dict(chg=7.0, vol3m=10_000, normal3m=1_000, price=10_100, vwap=10_000, idx_now=2_510, idx_open=2_500,
             hms="093000", prog5=100, prog_cum=5_000, cttr=150)
    m.update(kw)
    return m


class TestSignal(unittest.TestCase):
    def test_all_pass(self):
        full, base, _ = F.evaluate(metrics())
        self.assertTrue(full and base)

    def test_each_rule(self):
        cases = {"등락률": dict(chg=12.5), "거래량": dict(vol3m=1_999), "VWAP위": dict(price=9_990),
                 "VWAP괴리": dict(price=10_201), "지수": dict(idx_now=2_500), "시간": dict(hms="143001")}
        for name, kw in cases.items():
            full, base, checks = F.evaluate(metrics(**kw))
            self.assertFalse(checks[name], name)
            self.assertFalse(full or base, name)

    def test_flow_rules_only_affect_strategy(self):
        for kw in (dict(prog5=0), dict(prog5=None), dict(prog_cum=-1), dict(cttr=119.9)):
            full, base, _ = F.evaluate(metrics(**kw))
            self.assertFalse(full, kw)
            self.assertTrue(base, kw)             # 그림자 신호는 수급 조건을 보지 않음

    def test_boundaries(self):
        self.assertTrue(F.evaluate(metrics(chg=5.0))[0])
        self.assertTrue(F.evaluate(metrics(chg=12.0))[0])
        self.assertTrue(F.evaluate(metrics(price=10_200))[0])      # 괴리 정확히 +2%
        self.assertTrue(F.evaluate(metrics(cttr=120))[0])
        self.assertTrue(F.evaluate(metrics(hms="143000"))[0])
        self.assertTrue(F.evaluate(metrics(vol3m=2_000))[0])

    def test_normal_3min(self):
        self.assertAlmostEqual(F.normal_3min(23_400_000), 180_000)


class TestAccount(unittest.TestCase):
    def test_buy_25pct_and_cost(self):
        a = F.VirtualAccount()
        p = a.buy("A", 10_000, {}, "t", "d")
        self.assertEqual(p["qty"], int(2_500_000 // (10_000 * (1 + F.HALF))))
        self.assertAlmostEqual(a.cash, 10_000_000 - p["qty"] * 10_000 * (1 + F.HALF))

    def test_max_two_and_cash_limit(self):
        a = F.VirtualAccount(cash=3_000_000)
        a.positions["X"] = dict(qty=700, entry=10_000, last=10_000, peak=10_000, trough=10_000, overnight=False,
                                entry_time="t", entry_date="d", meta={})
        p = a.buy("A", 10_000, {}, "t", "d")       # 자산 1,000만의 25% = 250만, 현금 300만 → 250만
        self.assertEqual(p["qty"], int(2_500_000 // (10_000 * (1 + F.HALF))))
        self.assertIsNone(a.buy("B", 10_000, {}, "t", "d"))        # 3번째 종목 불가

    def test_trailing_and_return(self):
        a = F.VirtualAccount()
        a.buy("A", 10_000, {}, "t", "d")
        for px in (10_300, 10_500, 10_200):
            a.mark("A", px)
            self.assertFalse(a.trail_hit("A"))
        a.mark("A", 10_185)                                        # 10,500 × 0.97 = 10,185
        self.assertTrue(a.trail_hit("A"))
        tr = a.sell("A", 10_180, "t2", "추적손절")
        self.assertAlmostEqual(tr["ret_pct"], ((10_180 * (1 - F.HALF)) / (10_000 * (1 + F.HALF)) - 1) * 100)
        self.assertAlmostEqual(tr["max_pct"], 5.0)
        self.assertEqual(a.positions, {})

    def test_daily_limit(self):
        a = F.VirtualAccount()
        a.buy("A", 10_000, {}, "t", "d")
        a.buy("B", 10_000, {}, "t", "d")
        start = 10_000_000
        self.assertFalse(a.limit_breached(start, {"A": 9_700, "B": 9_700}))
        self.assertTrue(a.limit_breached(start, {"A": 9_500, "B": 9_500}))

    def test_decide_1520(self):
        self.assertTrue(F.decide_1520(9_800, 10_000))
        self.assertFalse(F.decide_1520(9_799, 10_000))


class TestSimMinute(unittest.TestCase):
    def bars(self, spec):
        return [(t, *v) for t, v in spec]

    def test_trailing_exit(self):
        b = self.bars([("093000", (100, 101, 99, 100)), ("093100", (100, 110, 100, 109)), ("093200", (109, 109, 106, 107))])
        r = F.sim_minute(b, "093000", 100)
        self.assertEqual(r["status"], "closed")
        self.assertAlmostEqual(r["exit"], 110 * 0.97)
        self.assertEqual(r["exit_hms"], "093200")

    def test_bearish_bar_high_then_low(self):
        b = self.bars([("093000", (100, 100, 100, 100)), ("093100", (104, 110, 100, 101))])   # 음봉: 고가 먼저
        r = F.sim_minute(b, "093000", 100)
        self.assertAlmostEqual(r["exit"], 110 * 0.97)

    def test_gap_exit_at_open(self):
        b = self.bars([("093000", (100, 100, 100, 100)), ("093100", (95, 96, 94, 95))])
        r = F.sim_minute(b, "093000", 100)
        self.assertEqual((r["exit"], r["reason"]), (95, "추적손절(갭)"))

    def test_overnight_and_1520(self):
        base = [("093000", (100, 100, 100, 100)), ("100000", (100, 105, 100, 104))]
        near = self.bars(base + [("151900", (104, 104, 103.5, 103.5)), ("152000", (103, 103, 90, 90))])
        self.assertEqual(F.sim_minute(near, "093000", 100)["status"], "overnight")    # 15:20 이후 분봉은 판단에 안 씀
        far = self.bars(base + [("151900", (103, 103, 102.5, 102.8))])
        r = F.sim_minute(far, "093000", 100)
        self.assertEqual((r["status"], r["exit"], r["reason"]), ("closed", 102.8, "15:20 청산"))

    def test_signal_minute_not_used(self):
        b = self.bars([("093000", (100, 100, 50, 100)), ("151900", (100, 100, 100, 100))])
        self.assertEqual(F.sim_minute(b, "093012", 100)["status"], "overnight")


class TestSeries(unittest.TestCase):
    def test_program_net_5min(self):
        s = F.ProgramSeries()
        for t, v in ((0, 100), (200, 150), (320, 400)):
            s.add(t, v)
        self.assertEqual(s.net_since(320), 300)          # 320-300=20초 시점 이전 값 = 0초의 100
        self.assertIsNone(s.net_since(250))             # 5분 전 이력 없음
        s.add(200, 160)                                 # 같은 시각은 덮어씀
        self.assertEqual(len(s.t), 3)

    def test_minute_bars(self):
        m = F.MinuteBars()
        for h, p in (("090001", 10), ("090030", 12), ("090059", 9), ("090100", 11)):
            m.add(h, p)
        self.assertEqual(m.rows(), [("090000", 10, 12, 9, 9), ("090100", 11, 11, 11, 11)])

    def test_refresh_tracker(self):
        r = F.RefreshTracker()
        self.assertEqual(r.seen("x", 1, 0), 0.0)
        self.assertIsNone(r.seen("x", 1, 5))
        self.assertEqual(r.seen("x", 2, 20), 20)
        self.assertEqual(r.summary()["x"]["median_sec"], 20)


class TestSafety(unittest.TestCase):
    def test_no_order_or_account_api_in_code(self):
        bad = re.compile(r"order-cash|order-credit|order-rvsecncl|inquire-balance|inquire-psbl|TTTC|VTTC|CANO|ACNT_PRDT|"
                         r"/trading/|hashkey|KIS_ACCOUNT_NO", re.I)
        for p in [HERE / "krx_flow_scalper.py", HERE / "flow_core.py", *(HERE / "vendor").glob("*.py")]:
            self.assertIsNone(bad.search(p.read_text(encoding="utf-8")), p.name)

    def test_rest_rejects_non_quote_path(self):
        r = S.KisRest("k", "s")
        with self.assertRaises(S.ApiError):
            r.get("/uapi/domestic-stock/v1/trading/order-cash", "X", {})

    def test_rate_limiter(self):
        rl = S.RateLimiter(5)
        t0 = time.monotonic()
        for _ in range(10):
            rl.wait()
        self.assertGreaterEqual(time.monotonic() - t0, 0.95)


class TestBot(unittest.TestCase):
    """네트워크 없이 Bot 의 체결·청산 흐름 확인(웹소켓은 시작하지 않음)."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        S.STATE_FILE, S.DATA_DIR, S.TOKEN_FILE = self.tmp / "state.json", self.tmp, self.tmp / "tok.json"
        self.b = S.Bot("k", "s")
        self.b.day = "20260928"
        self.b.st["date"] = "20260928"

    def tick(self, code, hms, px, bid=None, hi=None):
        self.b.book.update_trade({"MKSC_SHRN_ISCD": code, "STCK_PRPR": px, "BIDP1": bid or px - 10, "ASKP1": px + 10,
                                  "STCK_HGPR": hi or px, "STCK_CNTG_HOUR": hms, "PRDY_CTRT": 7, "ACML_VOL": 1})
        self.b.on_tick({"MKSC_SHRN_ISCD": code, "STCK_CNTG_HOUR": hms, "STCK_PRPR": str(px),
                        "BIDP1": str(bid or px - 10), "STCK_HGPR": str(hi or px)})

    def signal(self, code, ask=10_000):
        m = dict(metrics(), code=code, name=code, market="KOSPI", ask1=ask, bid1=ask - 10)
        self.b.enter(code, m, dict(m, date=self.b.day, sig_time=m["hms"]))

    def test_enter_trail_exit_logged(self):
        self.signal("A")
        self.assertIn("A", self.b.acct.positions)
        for hms, px in (("093100", 10_500), ("093150", 10_190), ("093200", 10_180)):   # 손절선 10,185
            self.tick("A", hms, px)
        self.b.flush_closed()
        rows = S.read_csv("trades.csv")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reason"], "추적손절")
        self.assertEqual(float(rows[0]["exit"]), 10_170)                    # 그 틱의 매수1호가
        self.assertEqual(rows[0]["prog5"], "100")                         # 신호 시점 지표 기록
        self.assertEqual(rows[0]["exit_time"], "093200")

    def test_third_signal_skipped_and_once_per_day(self):
        for c in ("A", "B", "C"):
            self.signal(c)
        acts = [r["action"] for r in S.read_csv("signals.csv")]
        self.assertTrue(acts[0].startswith("체결") and acts[1].startswith("체결"))
        self.assertEqual(acts[2], "자리 없음(2종목 보유)")
        self.assertEqual(sorted(self.b.st["signaled"]), ["A", "B", "C"])

    def test_1520_overnight_then_next_open(self):
        self.signal("A")
        self.tick("A", "151900", 10_400, hi=10_500)                        # 고가 -2% 이내
        self.b.force_1520()
        self.assertTrue(self.b.acct.positions["A"]["overnight"])
        self.tick("A", "152500", 9_000)                                   # 15:20 이후 틱은 손절 판단 안 함
        self.assertIn("A", self.b.acct.positions)
        self.b.day = "20260929"                                           # 다음날 첫 체결 → 매수1호가 매도
        self.tick("A", "090000", 10_600, bid=10_590)
        self.b.flush_closed()
        r = S.read_csv("trades.csv")[0]
        self.assertEqual((r["reason"], float(r["exit"]), r["exit_date"]), ("다음날 시가", 10_590, "20260929"))

    def test_1520_far_from_high_sells(self):
        self.signal("A")
        self.tick("A", "151900", 10_100, hi=10_400)
        self.b.force_1520()
        self.assertEqual(S.read_csv("trades.csv")[0]["reason"], "15:20 청산")

    def test_daily_limit_liquidates_and_halts(self):
        self.signal("A")
        self.signal("B")
        self.tick("A", "100000", 9_710)          # -2.9%, 추적손절 전
        self.tick("B", "100000", 9_710)
        self.b.st["day_start_equity"] = 10_000_000
        self.b.acct.positions["A"]["last"] = self.b.acct.positions["B"]["last"] = 9_710
        # 25%×2 = 50% 보유, -2.9% → 자산 -1.5% (한도 전)
        self.b.check_limit()
        self.assertFalse(self.b.st["halted"])
        self.b.st["day_start_equity"] = 10_070_000   # 시작 자산을 높여 -2% 초과 상황을 만듦
        self.b.check_limit()
        self.assertTrue(self.b.st["halted"])
        self.assertEqual(self.b.acct.positions, {})
        self.assertEqual({r["reason"] for r in S.read_csv("trades.csv")}, {"일일 손실 한도"})
        self.signal("C")
        self.assertEqual(S.read_csv("signals.csv")[-1]["action"], "일일 한도로 중단 중")

    def test_state_roundtrip_and_new_day(self):
        self.signal("A")
        self.b.acct.positions["A"]["overnight"] = True
        self.b.save_state()
        b2 = S.Bot("k", "s")                     # 오늘 날짜(실제)로 다시 읽기 → 새 거래일이면 초기화하되 보유는 유지
        self.assertIn("A", b2.acct.positions)
        self.assertAlmostEqual(b2.acct.cash, self.b.acct.cash)

    def test_run_sims_with_fake_bars(self):
        self.b.st["signal_open"] = {"A": dict(sig_time="093000", entry=100, name="A")}
        self.b.st["shadow_open"] = {"A": dict(sig_time="093000", entry=100, name="A"),
                                    "B": dict(sig_time="093000", entry=100, name="B")}
        bars = {"A": [("093000", 100, 100, 100, 100), ("093100", 100, 110, 100, 109), ("093200", 109, 109, 105, 106),
                      ("151900", 106, 106, 106, 106)],
                "B": [("093000", 100, 100, 100, 100), ("151900", 104, 104, 104, 104)]}
        self.b.minute_bars = lambda code, day: bars[code]
        n, pend = self.b.run_sims()
        self.assertEqual((n, pend), (3, 1))
        res = S.read_csv("sim_results.csv")
        self.assertEqual(sorted((r["kind"], r["code"], r["reason"]) for r in res),
                         [("shadow", "A", "추적손절"), ("signal", "A", "추적손절")])
        self.assertEqual(self.b.st["pending_sims"][0]["code"], "B")


if __name__ == "__main__":
    unittest.main()
