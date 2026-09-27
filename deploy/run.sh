#!/usr/bin/env bash
# 수급 단타 가상 봇 실행 루프 (systemd krx-flow-scalper.service 가 실행)
# 평일 08:50~15:45 사이에만 봇을 띄우고, 그날 작업이 끝났으면(state/done_YYYYMMDD) 다음 날까지 기다린다.
set -uo pipefail
APP_DIR="/opt/krx-flow-scalper"
cd "$APP_DIR"
[[ -f .env ]] || { echo "[ERROR] $APP_DIR/.env 없음"; exit 1; }
set -a; source .env; set +a
export PYTHONUNBUFFERED=1 TZ=Asia/Seoul
mkdir -p logs state data
while true; do
  DAY="$(date +%Y%m%d)"; DOW="$(date +%u)"; HM="$(date +%H:%M)"
  if [[ "$DOW" -le 5 && "$HM" > "08:49" && "$HM" < "15:45" && ! -f "state/done_$DAY" ]]; then
    echo "[$(date '+%F %T')] 봇 시작" >> "logs/flow_$DAY.log"
    .venv/bin/python krx_flow_scalper.py >> "logs/flow_$DAY.log" 2>&1
    echo "[$(date '+%F %T')] 봇 종료(코드 $?)" >> "logs/flow_$DAY.log"
    sleep 15
  else
    sleep 60
  fi
done
