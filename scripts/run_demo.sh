#!/usr/bin/env bash
# ChronosMatch end-to-end demo: launches the matching engine, market
# simulator, and headless latency monitor as three real OS processes
# communicating purely over shared-memory ring buffers, waits for the
# run to finish, then prints each process's log.
set -uo pipefail
cd "$(dirname "$0")/.."

DURATION=${1:-20}
BASE_RATE=${2:-300}
BURST_RATE=${3:-6000}
BURST_START=${4:-4}
BURST_DURATION=${5:-5}

rm -f /dev/shm/chronosmatch_*.ring
rm -f /tmp/chronosmatch_engine.log /tmp/chronosmatch_sim.log /tmp/chronosmatch_monitor.log

echo "== ChronosMatch demo: duration=${DURATION}s base=${BASE_RATE}/s burst=${BURST_RATE}/s@${BURST_START}s(+${BURST_DURATION}s) =="

python3 -u engine/engine_runner.py --duration "$DURATION" --idle-sleep-us 5 \
    > /tmp/chronosmatch_engine.log 2>&1 &
ENGINE_PID=$!

# Wait for the engine to actually create the ring buffers before starting
# the simulator/monitor (they attach as readers/writers, not creators).
for i in $(seq 1 100); do
    if [ -e /dev/shm/chronosmatch_orders.ring ] && [ -e /dev/shm/chronosmatch_trades.ring ]; then
        break
    fi
    sleep 0.05
done

python3 -u simulator/market_simulator.py --duration "$((DURATION - 2))" \
    --base-rate "$BASE_RATE" --burst-rate "$BURST_RATE" \
    --burst-start "$BURST_START" --burst-duration "$BURST_DURATION" \
    > /tmp/chronosmatch_sim.log 2>&1 &
SIM_PID=$!

python3 -u monitor/headless_monitor.py --duration "$((DURATION - 1))" \
    > /tmp/chronosmatch_monitor.log 2>&1 &
MON_PID=$!

wait "$SIM_PID"
wait "$MON_PID"
wait "$ENGINE_PID"

echo ""
echo "================= SIMULATOR LOG ================="
cat /tmp/chronosmatch_sim.log
echo ""
echo "================= ENGINE LOG ================="
cat /tmp/chronosmatch_engine.log
echo ""
echo "================= MONITOR LOG (latency) ================="
cat /tmp/chronosmatch_monitor.log

rm -f /dev/shm/chronosmatch_*.ring
