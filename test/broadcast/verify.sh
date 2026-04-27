#!/usr/bin/env bash
# End-to-end verification for the /rtc/broadcast feature, driving the
# mix leg through a lightweight ffmpeg WHIP publisher (no Chrome).
#
# This is the default verify target: it runs everywhere ffmpeg >= 7.1
# is available, has no browser dependency, and completes in well under
# a minute. For the real avflow Chrome-driven pipeline use
# `make verify-broadcast-avflow`.
#
# Pipeline:
#   1. Build axonkit + token CLIs.
#   2. Start axonkit on loopback (axonkit boots its own in-process NATS
#      + JetStream, so there is no external broker dependency).
#   3. Publish two fake room users (userA: red / A-440Hz, userB: green
#      / A-880Hz) into the room via WHIP. VIDEO_CODEC controls whether
#      the publishers use H.264 (default), VP9 or AV1 — the same codec
#      is forced on the mix publisher so axonkit's egress path is
#      exercised end-to-end for that codec.
#   4. Create a broadcast targeting a local ffmpeg RTMP listener with
#      liveIdentity=userA so the recording starts as solid red.
#   5. After the live warm-up, push a "mix" stream (solid blue,
#      1000 Hz) to axonkit's /rtc/broadcast/{id}/whip. The switcher
#      flips to mix; the recording should flip red→blue.
#   6. Stop the mix publisher (and explicitly DELETE the mix ingest to
#      short-circuit ICE timeout). Switcher flips back to live; the
#      recording flips blue→red.
#   7. Stop everything, concat captured FLV segments, transcode to MP4
#      into test/broadcast/out/<variant>-<stamp>/, and run mean-RGB
#      assertions on each phase.
#
# Because the mix here is a single-source ffmpeg publisher there is no
# composite layout to verify — the halves-check is skipped. The avflow
# variant covers the composite path.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# lib.sh requires VARIANT to be set so its log file names, MP4 basenames
# and final banner stay distinct from the avflow variant's.
VARIANT="ffmpeg"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib.sh"

lib_init
lib_build_axonkit
lib_start_axonkit
lib_mint_tokens

# Mix publisher authenticates as a room admin — the broadcast mix
# ingest endpoint requires admin, not just publish.
TOKEN_MIX_PUB="$TOKEN_ADMIN"

lib_start_recorder
lib_start_publishers
lib_wait_settle

lib_create_broadcast

echo "[phase-1] live-only ($WARMUP_LIVE s) → recording should show red"
sleep "$WARMUP_LIVE"

# ---------------------------------------------------------------------------
# Mix phase: an ffmpeg WHIP publisher that stands in for avflow's
# WHIPPushSink. Pushes a solid-blue 1000 Hz tone to
# /rtc/broadcast/{id}/whip. The codec + resolution + frame rate match
# the live publishers exactly — this is how a real mixer should behave
# (same pipeline parameters so axonkit's RTMP pusher sees byte-
# identical SPS/PPS across live↔mix flips and does NOT re-emit a mid-
# stream SeqHeader, which would drop the RTMP receiver's TCP socket).
# ---------------------------------------------------------------------------

MIX_LOG="$LOG_DIR/mix-whip.log"
echo "[mix-whip] starting solid-blue 1000Hz publisher → /rtc/broadcast/$BID/whip (codec=$VIDEO_CODEC)"
MIX_URL="$BASE_URL/rtc/broadcast/$BID/whip"
# Mix canvas is deliberately the same 320x240@30 as the live publishers
# (lib_start_publishers) — see header comment for rationale.
MIX_VIDEO_SRC="color=blue:size=320x240:rate=30"
MIX_AUDIO_SRC="sine=frequency=1000:sample_rate=48000,aresample=async=1:first_pts=0"
case "$VIDEO_CODEC" in
    h264)
        # ffmpeg's own -f whip muxer is H.264-only; for the h264
        # variant that's exactly what we want — identical publisher
        # path to the previous verify.sh baseline.
        ffmpeg -hide_banner -loglevel warning -nostats \
            -re \
            -f lavfi -i "$MIX_VIDEO_SRC,format=yuv420p" \
            -f lavfi -i "$MIX_AUDIO_SRC" \
            $(_lib_video_encode_args) \
            -c:a libopus -b:a 64k -ar 48000 -ac 2 \
            -authorization "$TOKEN_MIX_PUB" \
            -f whip "$MIX_URL" \
            >"$MIX_LOG" 2>&1 &
        MIX_PID=$!
        ;;
    vp8|vp9|av1)
        # ffmpeg → IVF+Ogg FIFOs → whipsim (pion WHIP client) — the
        # only way to push VP8/VP9/AV1 into axonkit because ffmpeg WHIP
        # refuses non-H.264 video. Note that VP8 may not reach RTMP
        # destinations (Enhanced-RTMP v2 has no VP8 FourCC); the mix
        # leg here is exercised over WHIP only.
        MIX_PID=$(_lib_whip_publish_whipsim \
            "$MIX_VIDEO_SRC" \
            "$MIX_AUDIO_SRC" \
            "$TOKEN_MIX_PUB" \
            "$MIX_LOG" \
            "mix-whip" \
            "$MIX_URL")
        ;;
    *)
        echo "unsupported VIDEO_CODEC=$VIDEO_CODEC" >&2
        exit 1
        ;;
esac
lib_track_pid "$MIX_PID"

if ! lib_wait_source "mix" "$MIX_FLIP_TIMEOUT"; then
    echo "mix publisher log tail:" >&2
    tail -40 "$MIX_LOG" >&2
    exit 1
fi

echo "[phase-2] mix active ($MIX_STABLE s) → recording should flip to blue"
sleep "$MIX_STABLE"

echo "[mix-whip] stopping publisher pid=$MIX_PID"
kill -TERM "$MIX_PID" 2>/dev/null || true
# Wait briefly for ffmpeg to flush its DELETE. If the publisher was
# killed mid-RTP, the switcher would still have to wait for ICE consent
# failure — we short-circuit that by sending DELETE ourselves.
sleep 1
lib_delete_mix_ingest

if ! lib_wait_source "live" "$LIVE_FLIP_TIMEOUT"; then
    exit 1
fi

echo "[phase-3] detached ($POST_DETACH s) → recording should flip back to red"
sleep "$POST_DETACH"

lib_status_snapshot_to_log
lib_delete_broadcast
lib_stop_publishers_and_recorder

# Phase-2 assertion: mix stream is solid blue — high B channel,
# dominated over both R and G. The halves check is not meaningful for
# a single-source mix, so we don't pass a halves_check argument.
_mix_is_blue() {
    lib_check_blue "$1" "$2" "$3"
}

lib_finalize _mix_is_blue
