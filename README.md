# KRX 수급 단타 — 가상 체결 기록 봇

실전 키로 **시세만 조회**하고, 신호가 나면 **가상으로 체결한 것처럼 기록**하는 봇이에요.

- **주문 없음:** 실전 계좌 주문·잔고 API는 코드에 없어요(`test_flow.py`가 매번 검사해요). REST 호출은 시세·순위 경로만 허용돼요.
- **기존 봇과 분리:** 기존 추격봇(`/opt/krx-bot`, 모의투자)과 폴더·리눅스 사용자(`krxflow`)·키·상태 파일·서비스가 모두 따로예요.
- **재사용한 코드:** 기존 봇의 휴장일 판단·API 백오프(서킷브레이커)·텔레그램 함수와 웹소켓 모듈을 `vendor/`에 복사해서 써요. 원본은 수정하지 않았어요.

## 파일

| 경로 | 내용 |
|---|---|
| `krx_flow_scalper.py` | 스캐너·웹소켓 감시·신호·가상 체결·장 마감 후 작업 |
| `flow_core.py` | 순수 로직(신호 판정, 가상 계좌, 분봉 사후 계산) |
| `vendor/krx_common.py` | 기존 봇 `krx_realtime_autotrader.py`(ab0266b)에서 휴장일·백오프·텔레그램 함수만 복사 |
| `vendor/krx_realtime_ws.py` | 기존 봇 웹소켓 모듈 복사본 |
| `test_flow.py` | 단위 테스트(네트워크 없음) |
| `deploy/run.sh`, `deploy/krx-flow-scalper.service` | 실행 루프, systemd 서비스 |
| `.env` | 실전 조회 키 + 텔레그램(권한 600, git 제외) |
| `state/state.json` | 가상 계좌·보유·오늘 신호 목록 |
| `data/trades.csv` | 가상 체결 거래: 신호 시점 모든 지표, 체결가, 청산가, 수익률, 보유 중 최고·최저 |
| `data/signals.csv` | 전략 신호 전부(체결 / 자리 없음 / 한도 중단) |
| `data/shadow.csv` | 그림자 신호: 수급 조건(프로그램·체결강도)만 뺀 나머지를 만족한 신호 |
| `data/sim_results.csv` | 전략 신호·그림자 신호를 같은 규칙으로 분봉 사후 계산한 거래당 결과 |
| `data/index_min/YYYYMMDD_KOSPI.csv` | 지수 1분봉(장중 웹소켓으로 만들고, 마감 후 REST 최근 102분으로 대조) |
| `data/refresh/YYYYMMDD.json` | 순위·프로그램·가집계 API 갱신 주기 측정 요약 |
| `logs/flow_YYYYMMDD.log` | 실행 로그. `[갱신]` 줄이 API 내용이 바뀐 시점이에요 |

## 규칙 요약

- **후보:**
  - 시총 1000억 이상, 관리종목·정리매매·거래정지·ETF·ETN·SPAC 제외.
  - 등락률 +3% 이상이고 시장별 거래량 급증(거래증가율) 순위 상위 30.
  - 그리고 종목별 프로그램 당일 누적 순매수 > 0, 또는 외국인·기관 가집계 순매수 > 0(가집계는 09:31 이후 값이 있으면).
- **감시:** 웹소켓 41건 = 지수 2 + 종목 39. 등락률이 3% 아래로 식거나 신호가 끝난 종목은 빼고 새 후보로 바꿔요. 보유 종목은 계속 감시해요.
- **신호(모두 만족):**
  - 등락률 5~12%.
  - 3분 거래량 ≥ 전일 거래량 ÷ 23400 × 180 × 2.
  - VWAP 위, VWAP 괴리 +2% 이내.
  - 프로그램 5분 순매수 > 0, 당일 누적 순매수 > 0(10초마다 조회, 30초보다 오래된 값은 안 씀).
  - 체결강도 ≥ 120.
  - 해당 시장 지수(실시간 KOSPI·KOSDAQ) > 시가.
  - 09:00~14:30, 종목당 하루 1회.
- **가상 체결:** 매수는 매도1호가, 매도는 매수1호가(웹소켓 체결 메시지의 최우선 호가). 비용은 왕복 0.45%.
- **청산:**
  - 고점 대비 -3%면 틱마다 즉시 판단해서 매도.
  - 15:20에 가격이 당일 고가 -2% 이내면 다음날 첫 체결의 매수1호가에 팔고, 아니면 15:20에 팔아요.
- **자금:** 가상 1,000만 원, 종목당 자산 25%, 최대 2종목. 일일 -2%면 전량 청산하고 그날 신규 진입을 멈춰요.
- **호출 한도:** REST 초당 14건 이하(실측 한도 20건의 70%), 웹소켓 41건.

## 운영

```bash
# 상태·로그
systemctl status krx-flow-scalper
tail -f /opt/krx-flow-scalper/logs/flow_$(date +%Y%m%d).log

# 재시작 / 중지 / 시작
sudo systemctl restart krx-flow-scalper
sudo systemctl stop krx-flow-scalper
sudo systemctl start krx-flow-scalper
```

- 평일 08:50에 봇을 띄우고, 15:30 이후 사후 작업을 마치면 종료해요. 휴장일이면 바로 종료해요.
- 그날 작업이 끝나면 `state/done_YYYYMMDD`를 남기고 다음 날까지 기다려요.
- 중간에 죽으면 15초 뒤 다시 떠요. 가상 계좌·보유·오늘 신호 목록은 `state/state.json`에서 이어받아요.

## 테스트

```bash
cd /opt/krx-flow-scalper
.venv/bin/python -m unittest test_flow -v                 # 네트워크 없이, 임시 폴더에서
sudo -u krxflow bash -c 'set -a; . ./.env; set +a; unset TELEGRAM_BOT_TOKEN; .venv/bin/python krx_flow_scalper.py --smoke 15'
#   ↑ 실전 키로 조회만: 순위 1회, 후보 3종목 상태·프로그램·가집계, 웹소켓 15초 (상태·데이터 파일은 임시 폴더에)
```

## 백업과 되돌리기

- **설치 전 백업:** `/opt/krx-flow-scalper-backups/`에 tar로 있어요.
  - 기존 봇 쪽은 바꾼 게 없어서 되돌릴 것이 없어요(`/opt/krx-bot` git 상태 그대로).
- **가상 기록 백업:** `tar czf ~/flow-data-$(date +%F).tgz -C /opt/krx-flow-scalper state data logs`
- **완전히 되돌리기(봇 제거):**
  ```bash
  sudo systemctl disable --now krx-flow-scalper
  sudo rm /etc/systemd/system/krx-flow-scalper.service && sudo systemctl daemon-reload
  sudo tar czf /root/krx-flow-scalper-final-$(date +%F).tgz -C /opt krx-flow-scalper   # 기록 보관
  sudo rm -rf /opt/krx-flow-scalper && sudo userdel krxflow
  ```
- **가상 계좌만 초기화:** 서비스를 멈추고 `state/state.json`을 다른 이름으로 옮긴 뒤 다시 시작해요.

## 알려진 한계

- **가상 체결 가격:** "신호 시점 직전 체결 메시지의 최우선 호가"로 체결해요. 실제로 주문하면 호가 잔량·지연 때문에 더 나빠질 수 있어요.
- **다음날 시가 매도:** 09:00 동시호가 후 첫 체결 메시지의 매수1호가예요.
- **"프로그램 순매수 상위" 순위 API는 KIS에 없어요.** 후보마다 종목별 프로그램 매매를 조회해서 대신해요.
- **가집계는 하루 약 5번만 바뀌어요.** 장 초반에는 사실상 프로그램 조건만으로 후보가 정해져요.
- **3분 거래량은 감시를 시작한 뒤의 체결만 셀 수 있어요.** 감시 직후 3분 동안은 적게 잡혀요(보수적).
- **그림자 신호 결과는 분봉으로 사후 계산해요.** 분봉 안 가격 순서는 양봉 시가→저가→고가→종가, 음봉 시가→고가→저가→종가로 가정해요. 실제 가상 계좌(틱 기준)와는 조금 다를 수 있어서, 비교는 같은 방법으로 계산한 "전략 신호 vs 그림자 신호"로 해요.
