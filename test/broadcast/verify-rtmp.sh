#!/usr/bin/env bash
# End-to-end verification of the RTMP-ingest → broadcast → RTMP-egress
# pipeline. Push an H.264 (with B frames) + Opus stream into axonkit
# over RTMP, create a /rtc/broadcast targeting the just-published
# participant, and let the broadcast forward the participant's tracks
# to a local ffmpeg RTMP recorder. Assert the recorded MP4 plays back
# end-to-end without decode errors.
#
# What this exercises that verify.sh does not:
#   - axonkit's go-rtmp ingest (-rtmp-listen) and FLV demuxer
#   - the H.264 depacketizer's CTS handling on classic-FLV B frames
#     (verify.sh forces -bf 0 because WHIP forbids B frames)
#   - the Enhanced-RTMP "Opus" FourCC audio path on both ingest and
#     egress (ffmpeg writes E-RTMP audio when -c:a libopus + -f flv)
#
# Pipeline:
#   1. Build axonkit + token CLIs.
#   2. Start axonkit on loopback with RTMP ingest enabled (its embedded
#      NATS server boots automatically; no external broker).
#   3. Start a local ffmpeg RTMP listener that records what arrives at
#      rtmp://127.0.0.1:$DST_RTMP_PORT/live/out into FLV segments.
#   4. ffmpeg pushes H.264 (libx264, -bf 3, high profile, B frames
#      enabled) + Opus to rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/<room>?
#      token=<jwt>. axonkit registers the publisher as participant
#      "rtmpUser" with one Opus and one H.264 producer.
#   5. POST /rtc/broadcast with participantIdentity=rtmpUser and one
#      RTMP destination pointing at the recorder. The broadcast pulls
#      the participant's tracks via mediasoup consumers, depacketizes,
#      and pushes them out as FLV.
#   6. Stream for $RECORD_DURATION seconds, then stop ffmpeg, DELETE the
#      broadcast, stop the recorder.
#   7. Assemble captured FLV segments → MP4, run a full ffmpeg null
#      decode and ffprobe to confirm the MP4 has both streams, a sane
#      duration, and zero decode errors.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# VARIANT distinguishes log/output paths. lib.sh uses it for the per-run
# OUT_DIR subfolder ($OUT_DIR/<VARIANT>-<stamp>/) and log filenames.
VARIANT="rtmp"

# Port axonkit's RTMP ingest binds on. Default chosen to avoid clashing
# with DST_RTMP_PORT (11935 — the recorder) and any local rtmp server
# the developer might run on the canonical :1935. Override via env to
# run multiple verify-rtmp.sh in parallel.
: "${RTMP_PUSH_PORT:=11936}"
RTMP_LISTEN=":$RTMP_PUSH_PORT"
export RTMP_LISTEN

: "${RECORD_DURATION:=15}"
: "${RTMP_IDENTITY:=rtmpUser}"

# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib.sh"

lib_init
lib_build_axonkit
lib_start_axonkit
lib_mint_tokens

# Mint a publish-capable token for the RTMP publisher. axonkit's RTMP
# handler reads the token from the stream key (?token=<jwt>) and binds
# claims.Identity() onto the participant — so this is what shows up
# under participantIdentity in the /rtc/broadcast create body.
RTMP_TOKEN=$(_lib_mint_token "$RTMP_IDENTITY" "true" "true")

# The recorder is a long-running RTMP listener on $DST_RTMP_PORT that
# accepts whatever the broadcast pushes at rtmp://127.0.0.1:$DST_RTMP_PORT/live/out
# and writes seg-NNN.flv files. Same supervisor loop as verify.sh.
lib_start_recorder

# ---------------------------------------------------------------------------
# RTMP publisher: H.264 with B frames + Opus → axonkit ingest.
#
# Encoder choices:
#   - libx264 -profile:v high -bf 3 -b_strategy 1: explicitly enables
#     B frames (the whole point of this test) and lets x264 pick the
#     B-frame placement adaptively. The classic-FLV CompositionTime
#     field then carries non-zero PTS-DTS deltas, exercising
#     decodeSignedInt24BE in pkg/rtc/rtmp.parseClassicVideoTag and
#     the H264Depacketizer's CTS-aware packing.
#   - -g 60 -r 30: 2-second IDR period so the broadcast pusher has a
#     keyframe to attach the AVCDecoderConfigurationRecord to within
#     the recorder's connection window.
#   - libopus -ar 48000 -ac 2: ffmpeg writes Enhanced-RTMP audio with
#     FourCC "Opus" (axonkit's FLV demuxer accepts both classic
#     soundFormat=13 and ExAudio FourCC=Opus).
# ---------------------------------------------------------------------------
PUSH_URL="rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/${ROOM}?token=${RTMP_TOKEN}"
PUSH_LOG="$LOG_DIR/rtmp-push.log"
echo "[rtmp-push] starting H.264(B-frames)+Opus testsrc → rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/$ROOM (identity=$RTMP_IDENTITY)"
# Source: lavfi testsrc = colour bars + a per-frame counter and a
# scrolling indicator. Real inter-frame motion makes libx264 emit a
# representative I/P/B mix (a static colour would flatten to ~all
# skip-blocks and barely exercise the B-frame path) and the frame
# counter doubles as a visual chronometer when reviewing the recorded
# MP4 by eye.
ffmpeg -hide_banner -loglevel warning -nostats \
    -re \
    -f lavfi -i "testsrc=size=640x480:rate=30,format=yuv420p" \
    -f lavfi -i "sine=frequency=440:sample_rate=48000,aresample=async=1:first_pts=0" \
    -c:v libx264 -preset veryfast -profile:v high -bf 3 -b_strategy 1 \
    -g 60 -r 30 -pix_fmt yuv420p -b:v 800k \
    -c:a libopus -b:a 64k -ar 48000 -ac 2 \
    -f flv "$PUSH_URL" \
    >"$PUSH_LOG" 2>&1 &
RTMP_PUSH_PID=$!
lib_track_pid "$RTMP_PUSH_PID"

# Give the publisher time to complete the RTMP handshake, push the AVC
# sequence header + the first IDR, and have axonkit fully wire up both
# producers. /rtc/broadcast checks publishing-status synchronously so
# we must not race ahead of the producers being created on the room.
echo "[rtmp-push] waiting for participant to publish (sleep 5)"
sleep 5

# ---------------------------------------------------------------------------
# Broadcast creation: pull the rtmpUser participant's tracks out and
# fan them to the recorder. The participant identity here is whatever
# the JWT carried (RTMP_IDENTITY); ParticipantIdentity is the only
# join key Broadcast.Start uses to match against the room roster.
# ---------------------------------------------------------------------------
DEST_URL="rtmp://127.0.0.1:$DST_RTMP_PORT/live/out"
echo "[broadcast] create: room=$ROOM liveIdentity=$RTMP_IDENTITY → $DEST_URL"
create_body=$(cat <<EOF
{
  "room": "$ROOM",
  "participantIdentity": "$RTMP_IDENTITY",
  "destinations": [
    { "type": "rtmp", "url": "$DEST_URL" }
  ]
}
EOF
)
resp=$(curl -sS -X POST "$BASE_URL/rtc/broadcast" \
    -H "Authorization: Bearer $TOKEN_ADMIN" \
    -H "Content-Type: application/json" \
    -d "$create_body")
echo "$resp" > "$LOG_DIR/broadcast-create.json"
BID=$(printf '%s' "$resp" | awk -F'"' '/"id"/ {print $4; exit}')
if [[ -z "$BID" || "$BID" == *error* ]]; then
    echo "failed to create broadcast: $resp" >&2
    echo "axonkit log tail:" >&2
    tail -40 "$LOG_DIR/axonkit.log" >&2
    exit 1
fi
echo "[broadcast] id=$BID"
T0=$(date +%s)

# No mix is attached, so the switcher should already be on the live
# source. Wait briefly to confirm — this surfaces participant lookup
# failures (bad identity, race) immediately rather than at recording
# time.
if ! lib_wait_source "live" 15; then
    echo "broadcast never reached source=live; ffmpeg push log tail:" >&2
    tail -40 "$PUSH_LOG" >&2
    exit 1
fi

echo "[record] streaming for ${RECORD_DURATION}s"
sleep "$RECORD_DURATION"

# ---------------------------------------------------------------------------
# Teardown. Send DELETE to the broadcast first so the egress closes
# cleanly and the recorder gets EOF, otherwise the recorder ffmpeg
# would sit on its TCP socket until our SIGTERM cuts it off and the
# trailing FLV bytes might be missing.
# ---------------------------------------------------------------------------
echo "[rtmp-push] stopping ffmpeg pid=$RTMP_PUSH_PID"
kill -TERM "$RTMP_PUSH_PID" 2>/dev/null || true
sleep 1
lib_status_snapshot_to_log
lib_delete_broadcast
sleep 1
lib_stop_publishers_and_recorder

# ---------------------------------------------------------------------------
# Verify the recording. lib_assemble_flv concatenates whatever the
# recorder captured into REC_DIR/out.flv (one segment is the happy
# path; multiple means the recorder reconnected mid-stream and the
# concat may stitch over a bad codec-config flip).
# ---------------------------------------------------------------------------
flv=$(lib_assemble_flv) || exit 1

stamp=$(date +%Y%m%d-%H%M%S)
dest_dir="$OUT_DIR/$VARIANT-$stamp"
mp4_path="$dest_dir/recording.mp4"
mkdir -p "$dest_dir"

echo "[verify] transcode FLV → MP4 ($mp4_path)"
ffmpeg -hide_banner -loglevel error -y -i "$flv" -c copy "$mp4_path"

duration=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$mp4_path" \
    | awk '{printf "%d", $1}')
v_codec=$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "$mp4_path")
a_codec=$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name -of csv=p=0 "$mp4_path")
v_streams=$(ffprobe -v error -select_streams v -show_entries stream=codec_name -of csv=p=0 "$mp4_path" | wc -l | tr -d ' ')
a_streams=$(ffprobe -v error -select_streams a -show_entries stream=codec_name -of csv=p=0 "$mp4_path" | wc -l | tr -d ' ')
has_b_frames=$(ffprobe -v error -select_streams v:0 -show_entries stream=has_b_frames -of csv=p=0 "$mp4_path")

echo "[verify] duration=${duration}s video=$v_codec audio=$a_codec has_b_frames=$has_b_frames"

fail=0
# Recording duration is wall-clock minus startup + teardown overhead.
# Allow a generous 5s margin on top of RECORD_DURATION because the
# initial RTMP handshake + the trailing DELETE both eat a second or two.
min_duration=$(( RECORD_DURATION - 5 ))
if [[ "$duration" -lt $min_duration ]]; then
    echo "FAIL: duration=$duration s shorter than expected (>= $min_duration s)" >&2
    fail=1
fi
if [[ "$v_streams" -lt 1 || "$a_streams" -lt 1 ]]; then
    echo "FAIL: missing streams (video=$v_streams audio=$a_streams)" >&2
    fail=1
fi
if [[ "$v_codec" != "h264" ]]; then
    echo "FAIL: video codec=$v_codec (want h264)" >&2
    fail=1
fi
if [[ "$a_codec" != "opus" ]]; then
    echo "FAIL: audio codec=$a_codec (want opus)" >&2
    fail=1
fi
# has_b_frames is "0" / "1" / "2" — non-zero means the encoder used
# B frames, which is the whole point of this regression test.
if [[ "${has_b_frames:-0}" == "0" ]]; then
    echo "FAIL: ffprobe reports has_b_frames=0; B-frame path was not exercised" >&2
    fail=1
fi

# Full decode pass + frame-count sanity. The recording is "playable"
# when ffmpeg can decode every packet without hitting an actual
# stream error AND every packet's DTS is strictly monotonic AND there
# is at least one B-frame whose PTS != DTS (proving the egress reorder
# rebuilt the PTS-DTS skew the publisher's encoder produced rather
# than collapsing them like the legacy CTS=0 path used to).
decode_log="$dest_dir/decode.log"
echo "[verify] full decode pass → $decode_log"
ffmpeg -hide_banner -v error -i "$mp4_path" -f null - 2>"$decode_log" || true
if [[ -s "$decode_log" ]]; then
    echo "FAIL: decoder reported errors:" >&2
    cat "$decode_log" >&2
    fail=1
fi

declared_v=$(ffprobe -v error -select_streams v:0 -show_entries stream=nb_frames -of csv=p=0 "$mp4_path" || echo 0)
counted_v=$(ffprobe -v error -count_frames -select_streams v:0 -show_entries stream=nb_read_frames -of csv=p=0 "$mp4_path" || echo 0)
if [[ "$counted_v" -lt 1 ]]; then
    echo "FAIL: video stream decoded zero frames" >&2
    fail=1
elif [[ -n "$declared_v" && "$declared_v" -gt 0 && "$counted_v" -lt $((declared_v - 5)) ]]; then
    echo "FAIL: video decoded only $counted_v / $declared_v frames" >&2
    fail=1
fi
echo "[verify] decoded frames: video=$counted_v/$declared_v"

# DTS/PTS sanity: with B-frames present in a properly-timestamped
# recording (a) the per-packet DTS series must be strictly monotonic
# (so muxers / players never see the "non monotonically increasing
# dts" warning the legacy CTS=0 path produced), and (b) there must
# be at least one frame whose PTS differs from its DTS — the
# canonical fingerprint of a working CompositionTime offset. The
# previous egress always wrote CTS=0, so PTS==DTS held for every
# frame and players sorted samples in decode order, producing the
# "frames jump back" symptom this fix targets.
ts_csv="$dest_dir/frame-ts.csv"
ffprobe -v error -select_streams v:0 -show_entries packet=pts,dts -of csv=p=0 "$mp4_path" >"$ts_csv" 2>/dev/null || true
ts_total=$(wc -l <"$ts_csv" | tr -d ' ')
ts_diff_count=$(awk -F, 'NF>=2 && $1 != $2 {n++} END {print n+0}' "$ts_csv")
ts_nonmono=$(awk -F, 'NF>=2 {if (NR>1 && $2+0 <= prev) n++; prev=$2+0} END {print n+0}' "$ts_csv")
echo "[verify] timestamps: packets=$ts_total pts!=dts=$ts_diff_count non-monotonic-dts=$ts_nonmono"
if [[ "${has_b_frames:-0}" != "0" && "$ts_diff_count" -lt 1 ]]; then
    echo "FAIL: B-frames present but every packet has PTS == DTS" >&2
    echo "       (egress dropped the CompositionTime reorder; players will show frames in decode order)" >&2
    fail=1
fi
if [[ "$ts_nonmono" -gt 0 ]]; then
    echo "FAIL: $ts_nonmono packets have non-monotonic DTS" >&2
    fail=1
fi

if [[ $fail -ne 0 ]]; then
    echo "verify-rtmp: FAILED (recording saved to $mp4_path for manual review)" >&2
    exit 1
fi

cat <<EOF

==============================================================
 verify-broadcast[rtmp]: OK
   ingest    : rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/$ROOM (h264+opus, b-frames)
   recording : $mp4_path
   duration  : ${duration}s  (video=$v_codec, audio=$a_codec, has_b_frames=$has_b_frames)
   logs      : $LOG_DIR
==============================================================
EOF
