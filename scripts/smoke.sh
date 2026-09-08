#!/usr/bin/env bash
# agent Connect-RPC smoke: drives the live agent over the easylab gateway's
# /agent.v1.* Connect endpoints (h2c/h1 dual-stack). Uses the Connect JSON
# codec (application/json, unary) so curl needs no protobuf.
#
#   AGENT_BASE=http://easylab.temp.svc.cluster.local:80 bash smoke.sh
set -uo pipefail
AGENT_BASE="${AGENT_BASE:-http://easylab.temp.svc.cluster.local:80}"
TOKEN="${TOKEN:-devtoken}"
SID="smoke-$(date +%s)$RANDOM"
PASS=0; FAIL=0
pass(){ echo "    PASS: $1"; PASS=$((PASS+1)); }
fail(){ echo "    FAIL: $1"; FAIL=$((FAIL+1)); }
H_AUTH="Authorization: Bearer $TOKEN"

# 1. create session (Connect unary, JSON)
body=$(curl -sf -X POST -H "$H_AUTH" -H 'Content-Type: application/json' \
  -d "{\"name\":\"$SID\"}" "$AGENT_BASE/agent.v1.AgentService/CreateSession")
echo "$body" | grep -qi 'sessionName\|session_name\|ok' && pass "create session" || fail "create session ($body)"

# 2. prompt (server-streaming; just POST the first message, drain a bit)
body=$(curl -sf -X POST -H "$H_AUTH" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$SID\",\"prompt\":\"reply with exactly: SMOKE-OK\"}" "$AGENT_BASE/agent.v1.AgentService/Prompt")
echo "$body" | grep -q 'accepted\|ok' && pass "submit prompt" || fail "submit prompt ($body)"

# 3. poll state until idle (Connect State unary)
state="busy"; for _ in $(seq 1 30); do
  state=$(curl -sf -X POST -H "$H_AUTH" -H 'Content-Type: application/json' \
    -d "{\"id\":\"$SID\"}" "$AGENT_BASE/agent.v1.AgentService/State" \
    | sed -E 's/.*"status":"([a-z]+)".*/\1/')
  [ "$state" = "idle" ] && break; sleep 2
done
check() { [ "$2" = "$3" ] && pass "$1" || fail "$1 (got '$2', want '$3')"; }
check "turn reaches idle" "$state" "idle"

# 4. messages (Connect ListMessages unary)
msgs=$(curl -sf -X POST -H "$H_AUTH" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$SID\",\"limit\":50}" "$AGENT_BASE/agent.v1.AgentService/ListMessages")
echo "$msgs" | grep -q 'assistant' && pass "assistant reply persisted" || fail "no assistant reply ($msgs)"
echo "$msgs" | grep -qi 'SMOKE-OK' && pass "sentinel echoed" || fail "sentinel missing ($msgs)"

echo "[agent-smoke] PASS=$PASS FAIL=$FAIL (session=$SID)"
[ "$FAIL" -eq 0 ]
