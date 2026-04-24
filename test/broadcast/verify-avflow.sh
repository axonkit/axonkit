#!/usr/bin/env bash
# End-to-end verification for the /rtc/broadcast feature, driving the
# mix leg through a real avflow pipeline (headless Chrome + WHIP).
#
# This variant proves the exact production integration works: avflow
# subscribes to the room, its video_mixer + audio_mixer composite
# userA + userB into a side-by-side grid, and WHIPPushSink pushes the
# result into axonkit's broadcast mix ingest with passthrough H.264 +
# Opus. The ffmpeg variant (verify.sh) is the fast default; run this
# one when you've touched avflow, WHIPPushSink, or the mix-ingest
# side of axonkit.
#
# Pipeline:
#   1. Build axonkit CLIs + the avflow binary (web bundle must already
#      be built — `cd $AVFLOW_REPO/web && pnpm build` once).
#   2. Start axonkit (embedded in-process NATS) + two WHIP publishers (red userA, green
#      userB), same as verify.sh.
#   3. Start avflow in HTTP-only mode so Chrome is warmed up by the
#      time we submit a job.
#   4. After the live warm-up, POST a job to avflow's /jobs API whose
#      pipeline is rtc_room[userA,userB] → video_mixer(grid) +
#      audio_mixer → whip_push into the broadcast's mix ingest. The
#      switcher flips to mix.
#   5. DELETE the avflow job then DELETE /rtc/broadcast/{id}/whip so
#      the switcher flips back to live even if Chrome's teardown
#      races the fetch(DELETE) from WHIPPushSink.
#   6. Finalise recording + run phase assertions, including a
#      left/right half check on the composite frame.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

VARIANT="avflow"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib.sh"

# avflow control API + canvas knobs ------------------------------------------
#
# MIX_CANVAS_{W,H,FPS} default to the SAME resolution/frame rate as the
# live publishers spawned by lib_start_publishers (320x240@30). Keeping
# them in lockstep means avflow's mix WHIP egress negotiates the same
# codec config as the live users, so axonkit's RTMP pusher will see a
# byte-identical SPS/PPS on the live↔mix flip and skip re-emitting the
# FLV SeqHeader tag — that avoids the ffmpeg RTMP receiver dropping
# the TCP connection mid-stream. Override these env vars if you want
# to test the codec-change / re-connect path explicitly.
: "${AVFLOW_CONTROL_PORT:=18090}"
: "${MIX_CANVAS_W:=320}"
: "${MIX_CANVAS_H:=240}"
: "${MIX_CANVAS_FPS:=30}"

AVFLOW_REPO="${AVFLOW_REPO:-$REPO_ROOT/../avflow}"
AVFLOW_BIN="${AVFLOW_BIN:-$AVFLOW_REPO/bin/avflow}"
AVFLOW_URL="http://127.0.0.1:$AVFLOW_CONTROL_PORT"

lib_init
lib_build_axonkit

# Build avflow -----------------------------------------------------------------
if [[ ! -d "$AVFLOW_REPO" ]]; then
    echo "avflow repo not found at $AVFLOW_REPO" >&2
    echo "set AVFLOW_REPO=... to point at your avflow checkout" >&2
    exit 1
fi
echo "[build] avflow CLI (skips web bundle; assumes pnpm build was run)"
(cd "$AVFLOW_REPO" && go build -o bin/avflow ./cmd/avflow) >/dev/null
if [[ ! -x "$AVFLOW_BIN" ]]; then
    echo "avflow binary missing at $AVFLOW_BIN after build" >&2
    exit 1
fi
if [[ ! -s "$AVFLOW_REPO/pkg/server/static/index.html" ]]; then
    echo "avflow web bundle missing; run 'cd $AVFLOW_REPO/web && pnpm build' once" >&2
    exit 1
fi

lib_start_axonkit
lib_mint_tokens

# avflow's rtc_room source joins axonkit as a regular subscribe-only
# participant. It needs canSubscribe; canPublish stays off so avflow's
# presence never gets mirrored back into its own composite.
TOKEN_AVFLOW_ROOM=$(lib_mint_subscriber_token "avflow-mixer")

lib_start_recorder
lib_start_publishers

# avflow in HTTP-only mode pre-starts Chrome so that the mixing-job
# submission later only pays for "connect + subscribe + composite +
# WHIP" latency, not Chrome's 3–6 s cold-start. We start it in parallel
# with the publishers since Chrome launch doesn't depend on axonkit.
echo "[avflow] starting on :$AVFLOW_CONTROL_PORT (headless Chrome warm-up)"
"$AVFLOW_BIN" --control ":$AVFLOW_CONTROL_PORT" --debug \
    >"$LOG_DIR/avflow.log" 2>&1 &
AVFLOW_PID=$!
lib_track_pid "$AVFLOW_PID"

lib_wait_settle
lib_create_broadcast

echo "[phase-1] live-only ($WARMUP_LIVE s) → recording should show red"
sleep "$WARMUP_LIVE"

# Wait for avflow's control API to be up before submitting. In
# HTTP-only mode avflow starts Chrome first and only listens after the
# browser WebSocket bridge connects, so this also confirms Chrome
# booted successfully.
echo "[avflow] waiting for control API at $AVFLOW_URL ..."
for _ in $(seq 1 200); do
    if curl -fsS -o /dev/null "$AVFLOW_URL/jobs" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$AVFLOW_PID" 2>/dev/null; then
        echo "avflow exited during startup; tail of log:" >&2
        tail -60 "$LOG_DIR/avflow.log" >&2
        exit 1
    fi
    sleep 0.25
done
if ! curl -fsS -o /dev/null "$AVFLOW_URL/jobs" 2>/dev/null; then
    echo "avflow control API never came up; tail of log:" >&2
    tail -60 "$LOG_DIR/avflow.log" >&2
    exit 1
fi

# Submit the mixing job -------------------------------------------------------
#
# Pipeline:
#   rtc_room[userA,userB] --> video_mixer(grid 2-col) --> whip_push -> axonkit
#                          \-> audio_mixer          --/
#
# Forcing videoCodec=h264 in the whip_push encoding config makes
# WHIPPushSink pin H264 via setCodecPreferences — axonkit's broadcast
# mix ingest only depacketizes the subset we validate here.
JOB_NAME="mix-$ROOM"
JOB_JSON="$STATE_DIR/avflow-job.json"
cat >"$JOB_JSON" <<EOF
{
  "name": "$JOB_NAME",
  "sources": [
    {
      "name": "room",
      "type": "rtc_room",
      "config": {
        "serverUrl": "$BASE_URL",
        "token": "$TOKEN_AVFLOW_ROOM",
        "roomName": "$ROOM",
        "select": { "mediaType": "both", "identities": ["userA", "userB"] }
      }
    }
  ],
  "nodes": [
    {
      "name": "video_mix",
      "type": "video_mixer",
      "inputs": ["room"],
      "config": {
        "canvas": {
          "width": $MIX_CANVAS_W,
          "height": $MIX_CANVAS_H,
          "fps": $MIX_CANVAS_FPS,
          "bgColor": "#000000"
        },
        "layout": {
          "mode": "grid",
          "common": { "padding": 0, "gap": 0, "defaultFit": "cover" },
          "grid": { "maxColumns": 2, "maxRows": 1, "sortBy": "joinedAt" }
        }
      }
    },
    {
      "name": "audio_mix",
      "type": "audio_mixer",
      "inputs": ["room"],
      "config": { "codec": "opus" }
    }
  ],
  "sinks": [
    {
      "name": "whip_out",
      "type": "whip_push",
      "inputs": ["video_mix", "audio_mix"],
      "config": {
        "url": "$BASE_URL/rtc/broadcast/$BID/whip",
        "headers": { "Authorization": "Bearer $TOKEN_ADMIN" },
        "encoding": {
          "videoCodec": "$VIDEO_CODEC",
          "audioCodec": "opus",
          "videoBitrateBps": 2000000,
          "audioBitrateBps": 128000
        }
      }
    }
  ]
}
EOF

echo "[avflow] submitting mix job ($JOB_NAME)"
AVFLOW_SUBMIT=$(curl -sS -X POST "$AVFLOW_URL/jobs" \
    -H "Content-Type: application/json" \
    --data @"$JOB_JSON" \
    -w "\nHTTP_STATUS=%{http_code}\n")
echo "$AVFLOW_SUBMIT" >"$LOG_DIR/avflow-submit.txt"
if ! echo "$AVFLOW_SUBMIT" | grep -q 'HTTP_STATUS=200'; then
    echo "avflow job submission failed:" >&2
    echo "$AVFLOW_SUBMIT" >&2
    exit 1
fi

if ! lib_wait_source "mix" "$MIX_FLIP_TIMEOUT"; then
    echo "avflow log tail:" >&2
    tail -80 "$LOG_DIR/avflow.log" >&2
    exit 1
fi

echo "[phase-2] mix active ($MIX_STABLE s) → recording should show userA+userB composite"
sleep "$MIX_STABLE"

# Detach mix. avflow's WHIPPushSink.stop() normally fires its own
# DELETE at axonkit's /rtc/broadcast/{id}/whip — but because the
# engine scheduleExit() races Chrome teardown, the fetch(DELETE) may
# be cancelled mid-flight when browser.Stop() kills Chrome. We send
# DELETE /jobs to avflow first (so Chrome attempts the clean path),
# then issue an explicit DELETE /rtc/broadcast/{id}/whip to axonkit so
# the switcher never has to rely on ICE consent failure to detect the
# ingest going away.
echo "[avflow] DELETE /jobs/$JOB_NAME"
curl -sS -X DELETE "$AVFLOW_URL/jobs/$JOB_NAME" -o /dev/null -w "status=%{http_code}\n" \
    | tee "$LOG_DIR/avflow-delete.txt"

lib_delete_mix_ingest

if ! lib_wait_source "live" "$LIVE_FLIP_TIMEOUT"; then
    exit 1
fi

echo "[phase-3] detached ($POST_DETACH s) → recording should flip back to red"
sleep "$POST_DETACH"

lib_status_snapshot_to_log
lib_delete_broadcast

# Stop avflow gracefully before killing Chrome via cleanup. The DELETE
# /jobs above already triggered engine shutdown; here we wait briefly
# for Chrome to exit so avflow.log captures any teardown errors.
if kill -0 "$AVFLOW_PID" 2>/dev/null; then
    sleep 1
    if kill -0 "$AVFLOW_PID" 2>/dev/null; then
        kill -TERM "$AVFLOW_PID" 2>/dev/null || true
    fi
fi

lib_stop_publishers_and_recorder

# Phase-2 assertion for avflow: the 2-cell grid of userA (red) +
# userB (green) should yield red+green means (check_mix_mean), and a
# half-frame check should have one side red-dominant and the other
# green-dominant (order is non-deterministic because both tracks join
# in the same tick).
_avflow_mix_check() {
    lib_check_mix_mean "$1" "$2" "$3"
}
_avflow_halves_check() {
    local l_rgb=$1 r_rgb=$2
    read -r lr lg lb <<<"$(lib_split_rgb "$l_rgb")"
    read -r rr rg rb <<<"$(lib_split_rgb "$r_rgb")"
    local left_red=0 left_green=0 right_red=0 right_green=0
    lib_check_red   "$lr" "$lg" "$lb" && left_red=1
    lib_check_green "$lr" "$lg" "$lb" && left_green=1
    lib_check_red   "$rr" "$rg" "$rb" && right_red=1
    lib_check_green "$rr" "$rg" "$rb" && right_green=1
    if (( (left_red == 1 && right_green == 1) || (left_green == 1 && right_red == 1) )); then
        return 0
    fi
    return 1
}

lib_finalize _avflow_mix_check _avflow_halves_check
