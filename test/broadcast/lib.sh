# Shared helpers for the broadcast verify scripts.
#
# Both verify.sh (ffmpeg WHIP simulator) and verify-avflow.sh (real
# avflow Chrome-based mixer) share ~80% of their orchestration: build
# axonkit, start axonkit (with its embedded in-process NATS + JetStream),
# publish two fake users, wire up an RTMP recorder, create a broadcast,
# run phase-1/2/3 assertions, finalise the recording. Only the mix
# phase differs. Factor the common bits here so fixes land once and
# the two scripts stay in sync.
#
# Callers must:
#   1. `source` this file.
#   2. Assign VARIANT ("ffmpeg" or "avflow") before calling helpers.
#   3. Call lib_* helpers in the order documented below.
#
# Every helper uses `set -euo pipefail` semantics from the caller and
# writes to BASH arrays / scalars that other helpers later read. No
# helper is re-entrant; verify.sh runs exactly once per invocation.

# ---------------------------------------------------------------------------
# Runtime knobs. The calling script may override any of these before calling
# lib_init, but must set VARIANT.
# ---------------------------------------------------------------------------
: "${VARIANT:?set VARIANT to ffmpeg or avflow before sourcing lib.sh}"

: "${ROOM:=verify-$(date +%s)}"
: "${API_KEY:=axonkit-verify}"
: "${API_SECRET:=$(openssl rand -hex 32)}"
: "${HTTP_PORT:=17990}"
: "${WEBRTC_MIN_PORT:=51200}"
: "${WEBRTC_MAX_PORT:=51400}"
: "${DST_RTMP_PORT:=11935}"
: "${ANNOUNCED_IP:=127.0.0.1}"

# RTMP_LISTEN is the bind address axonkit's RTMP push ingest listens on
# (matches `axonkit -rtmp-listen ...`). Empty disables ingest, which is
# what verify.sh / verify-avflow.sh want — they only need the broadcast
# RTMP egress path. verify-rtmp.sh sets this to ":<port>" so ffmpeg can
# push a real H.264+Opus stream into the room.
: "${RTMP_LISTEN:=}"

# Publisher / broadcast codec. H.264 is the classic FLV path; VP9 / AV1
# exercise the Enhanced-RTMP (E-RTMP) writer. Each codec pins the
# ffmpeg encoder + the mediasoup capability negotiation on both the
# live publishers and — for the ffmpeg variant — the mix simulator.
# Normalised to lowercase so that `VIDEO_CODEC=VP9` / `H264` / `Av1` all
# work on the command line without the user having to remember the
# internal convention.
: "${VIDEO_CODEC:=h264}"
VIDEO_CODEC="$(printf '%s' "$VIDEO_CODEC" | tr '[:upper:]' '[:lower:]')"

# Durations (seconds). Shared so both variants spend the same time in
# each phase; tolerances in the assertions depend on these.
: "${WARMUP_LIVE:=6}"
: "${MIX_STABLE:=15}"
: "${POST_DETACH:=6}"
: "${MIX_FLIP_TIMEOUT:=45}"
: "${LIVE_FLIP_TIMEOUT:=15}"

# Paths set by lib_init.
SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[1]}")" && pwd)}"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
BIN_AXONKIT="$REPO_ROOT/bin/axonkit"
BIN_TOKEN="$REPO_ROOT/bin/token"
WORKER_BIN="${WORKER_BIN:-$REPO_ROOT/bin/mediasoup-worker}"
BASE_URL="http://127.0.0.1:$HTTP_PORT"

# OUT_DIR is where the final verification artefacts (MP4 recording,
# frame PNGs, per-run frames.txt report) land. Kept inside the test
# tree so `.gitignore` can hide it and so nothing leaks into the
# user's ~/Downloads. Override OUT_DIR to redirect elsewhere.
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/out}"

# Tracked child PIDs (cleaned up in _lib_cleanup).
PIDS=()
lib_track_pid() { [[ -n "${1:-}" ]] && PIDS+=("$1"); }

# Populated by lib_init. STATE_DIR is the per-run scratch; LOG_DIR gets
# a file per subsystem (axonkit, publishers, avflow, recorder).
# NATS_STORE is the JetStream directory for axonkit's embedded NATS —
# we pin it inside STATE_DIR so a run never pollutes the repo with a
# stray .data/ directory.
STATE_DIR=""
LOG_DIR=""
REC_DIR=""
NATS_STORE=""

# Populated after lib_create_broadcast. T0 is wall-clock at broadcast
# creation; MIX_FLIP_REC / LIVE_FLIP_REC are seconds-since-T0 when the
# switcher reported each state.
T0=0
BID=""
T_MIX_FLIP=0
T_LIVE_FLIP=0
MIX_FLIP_LATENCY=0
LIVE_FLIP_LATENCY=0
MIX_FLIP_REC=0
LIVE_FLIP_REC=0

# Recorder supervisor pid (the bash loop; its ffmpeg child is a
# grandchild, reaped via pkill -P in cleanup).
RECORDER_LOOP_PID=0
RECORDER_LOOP_PID_FILE=""

# Publisher pids (set by lib_start_publishers).
PUB_A_PID=0
PUB_B_PID=0

# ---------------------------------------------------------------------------
# lib_init: create STATE_DIR, install the trap, verify required binaries
# that every variant needs. Variants that need extra binaries (avflow
# needs go/python3/avflow) call lib_require_bin themselves after this.
# ---------------------------------------------------------------------------
lib_init() {
    STATE_DIR="$(mktemp -d -t "axonkit-broadcast-verify-$VARIANT.XXXXXX")"
    LOG_DIR="$STATE_DIR/logs"
    REC_DIR="$STATE_DIR/rec"
    NATS_STORE="$STATE_DIR/nats"
    mkdir -p "$LOG_DIR" "$REC_DIR" "$NATS_STORE"
    RECORDER_LOOP_PID_FILE="$STATE_DIR/recorder.pid"
    trap _lib_cleanup EXIT INT TERM

    lib_require_bin ffmpeg
    lib_require_bin ffprobe
    lib_require_bin curl
    lib_require_bin openssl
    lib_require_bin awk
    lib_require_bin python3
    lib_require_bin go

    if ! ffmpeg -muxers 2>/dev/null | grep -q "whip"; then
        echo "ffmpeg is missing the WHIP muxer; need ffmpeg >= 7.1" >&2
        exit 1
    fi

    if [[ ! -x "$WORKER_BIN" ]]; then
        echo "mediasoup-worker binary not found at $WORKER_BIN" >&2
        echo "set WORKER_BIN=... or place it at bin/mediasoup-worker" >&2
        exit 1
    fi
}

lib_require_bin() {
    command -v "$1" >/dev/null 2>&1 || { echo "missing dependency: $1" >&2; exit 1; }
}

_lib_cleanup() {
    local rc=$?
    set +e
    echo "[cleanup] stopping child processes"
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            pkill -TERM -P "$pid" 2>/dev/null || true
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    sleep 1
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            pkill -KILL -P "$pid" 2>/dev/null || true
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    # The RTMP recorder supervisor respawns ffmpeg every time a connection
    # ends. By the time we SIGTERM the loop, a child ffmpeg may already be
    # listening on $DST_RTMP_PORT — if it survives the loop's death it
    # blocks the next verify-broadcast run. Reap anything still holding
    # the port.
    local stale_pid
    stale_pid=$(lsof -nPi ":$DST_RTMP_PORT" -t 2>/dev/null)
    if [[ -n "$stale_pid" ]]; then
        echo "[cleanup] killing stray listener on :$DST_RTMP_PORT pid=$stale_pid"
        kill -KILL $stale_pid 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    if [[ -n "$LOG_DIR" ]]; then
        echo "[cleanup] logs preserved at $LOG_DIR"
        echo "[cleanup] working dir: $STATE_DIR"
    fi
    exit "$rc"
}

# ---------------------------------------------------------------------------
# Build + boot axonkit/publishers/recorder. NATS runs embedded inside
# axonkit so there is no separate broker process to launch.
# ---------------------------------------------------------------------------

lib_build_axonkit() {
    echo "[build] axonkit + token + whipsim CLIs"
    (cd "$REPO_ROOT" && go build -o bin/axonkit ./cmd/axonkit) >/dev/null
    (cd "$REPO_ROOT" && go build -o bin/token ./cmd/token) >/dev/null
    # whipsim is the pion-based WHIP publisher used for VP9/AV1 ingest
    # because ffmpeg -f whip only speaks H.264. Building it every run
    # keeps the test self-contained and catches breakage in the
    # cmd/whipsim package alongside the rest of the pipeline.
    (cd "$REPO_ROOT" && go build -o bin/whipsim ./cmd/whipsim) >/dev/null
}

lib_start_axonkit() {
    # -nats "" selects the embedded in-process NATS server (see
    # pkg/server/server.go:NewServer), so verify runs have zero
    # external broker dependency. -nats-storedir pins JetStream to a
    # path inside STATE_DIR, otherwise the embedded server would
    # create a stray ./.data/ in the repo root.
    echo "[axonkit] starting on :$HTTP_PORT rtmp-listen='$RTMP_LISTEN' (worker=$WORKER_BIN, embedded nats storedir=$NATS_STORE)"
    "$BIN_AXONKIT" \
        -nats "" \
        -nats-storedir "$NATS_STORE" \
        -port "$HTTP_PORT" \
        -worker "$WORKER_BIN" \
        -rtmp-listen "$RTMP_LISTEN" \
        -api-key "$API_KEY" \
        -api-secret "$API_SECRET" \
        -webrtc-listen-ip "0.0.0.0" \
        -webrtc-announced-ip "$ANNOUNCED_IP" \
        -webrtc-min-port "$WEBRTC_MIN_PORT" \
        -webrtc-max-port "$WEBRTC_MAX_PORT" \
        >"$LOG_DIR/axonkit.log" 2>&1 &
    AXONKIT_PID=$!
    lib_track_pid "$AXONKIT_PID"

    local i
    for i in $(seq 1 100); do
        if curl -fsS -o /dev/null "$BASE_URL/rtc/join" -X OPTIONS 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$AXONKIT_PID" 2>/dev/null; then
            echo "axonkit exited during startup; tail of log:" >&2
            tail -40 "$LOG_DIR/axonkit.log" >&2
            exit 1
        fi
        sleep 0.1
    done
    echo "axonkit failed to become healthy" >&2
    exit 1
}

# lib_mint_tokens expects no args. Exports TOKEN_A, TOKEN_B, TOKEN_ADMIN.
# Additional tokens (e.g., TOKEN_AVFLOW_ROOM) are minted in variant scripts.
lib_mint_tokens() {
    TOKEN_A=$(_lib_mint_token "userA" "true" "true")
    TOKEN_B=$(_lib_mint_token "userB" "true" "true")
    TOKEN_ADMIN=$(_lib_mint_token "broadcast-admin" "true" "true")
}

_lib_mint_token() {
    local identity=$1 publish=$2 subscribe=$3
    "$BIN_TOKEN" create \
        -api-key "$API_KEY" \
        -api-secret "$API_SECRET" \
        -identity "$identity" \
        -room "$ROOM" \
        -admin=true \
        -publish="$publish" \
        -subscribe="$subscribe"
}

# Mint a subscribe-only token for a non-publishing participant (used by
# avflow's rtc_room source so its presence doesn't loop back into the
# mix).
lib_mint_subscriber_token() {
    local identity=$1
    "$BIN_TOKEN" create \
        -api-key "$API_KEY" \
        -api-secret "$API_SECRET" \
        -identity "$identity" \
        -room "$ROOM" \
        -admin=false \
        -publish=false \
        -subscribe=true
}

# ---------------------------------------------------------------------------
# RTMP receiver (broadcast destination).
#
# When the live↔mix flip carries a different video codec config
# (SPS/PPS bytes differ, or the FourCC changes), ffmpeg's RTMP demuxer
# treats the mid-stream SeqHeader tag as a codec change and drops its
# TCP connection. axonkit avoids emitting that tag when the codec
# config bytes don't actually change (see rtmpPusher.EnsureVideoSeqHeader
# skip path), and the verify scripts align the live + mix publishers
# to share the same resolution / encoder so the happy path is a single
# uninterrupted session. The supervisor loop here is kept as a defence
# in depth — if someone explicitly drives live and mix with different
# codec configs, each reconnect is captured as its own FLV segment and
# all segments are concatenated into out.flv after the run.
# ---------------------------------------------------------------------------
lib_start_recorder() {
    echo "[recorder] persistent RTMP listener on rtmp://127.0.0.1:$DST_RTMP_PORT/live/out"
    (
        seq=0
        while true; do
            seq=$((seq + 1))
            segment="$REC_DIR/seg-$(printf '%03d' "$seq").flv"
            ffmpeg -hide_banner -loglevel warning -nostats \
                -rtmp_listen 1 -listen_timeout 60 -timeout 60 \
                -i "rtmp://127.0.0.1:$DST_RTMP_PORT/live/out" \
                -c copy -y "$segment" \
                >>"$LOG_DIR/recorder.log" 2>&1 || true
            sleep 0.2
        done
    ) &
    RECORDER_LOOP_PID=$!
    echo "$RECORDER_LOOP_PID" > "$RECORDER_LOOP_PID_FILE"
    lib_track_pid "$RECORDER_LOOP_PID"
    sleep 0.5
}

# ---------------------------------------------------------------------------
# WHIP publishers for the two fake room users.
#
# Each publisher sends a solid colour (userA=red, userB=green) and a
# distinct sine tone (userA=440 Hz, userB=880 Hz). The video codec is
# controlled by VIDEO_CODEC so the Enhanced-RTMP path can be exercised
# without changing the ffmpeg invocation between variants.
# ---------------------------------------------------------------------------
_lib_video_encode_args() {
    case "$VIDEO_CODEC" in
        h264)
            # WebRTC / WHIP forbids B frames (libavformat/whip.c rejects
            # par->video_delay > 0 with "Unsupported B frames by RTC").
            # Keep -bf 0 regardless of profile so a future profile change
            # doesn't silently re-introduce reorder latency.
            echo "-c:v libx264 -preset ultrafast -tune zerolatency -profile:v main -bf 0 -pix_fmt yuv420p -g 30 -r 30"
            ;;
        vp8)
            # libvpx in real-time mode. -deadline realtime + -cpu-used 4
            # match what WebRTC's libvpx encoder does in browsers, and
            # -auto-alt-ref 0 keeps the encoder strictly causal so the
            # IVF stream is safe to feed straight into whipsim's RTP
            # packetizer (which has no reorder buffer).
            echo "-c:v libvpx -deadline realtime -cpu-used 4 -pix_fmt yuv420p -g 30 -r 30 -b:v 1M -auto-alt-ref 0 -lag-in-frames 0"
            ;;
        vp9)
            # libvpx-vp9 in real-time mode. deadline=realtime + cpu-used=8
            # matches what WebRTC encoders do by default, so ffmpeg's
            # output is representative of "a browser publishing VP9".
            # row-mt + tile-columns keep encode latency bounded.
            echo "-c:v libvpx-vp9 -deadline realtime -cpu-used 8 -row-mt 1 -tile-columns 2 -frame-parallel 1 -pix_fmt yuv420p -g 30 -r 30 -b:v 1M -auto-alt-ref 0 -lag-in-frames 0"
            ;;
        av1)
            # libsvtav1 in low-latency mode. preset 12 is the fastest
            # real-time AV1 encoder ffmpeg ships that still produces
            # a decodable bitstream.
            echo "-c:v libsvtav1 -preset 12 -svtav1-params tune=0:lookahead=0:fast-decode=1 -pix_fmt yuv420p -g 30 -r 30 -b:v 1M"
            ;;
        *)
            echo "unsupported VIDEO_CODEC=$VIDEO_CODEC" >&2
            return 1
            ;;
    esac
}

# _lib_whip_publish_ffmpeg pushes via ffmpeg's -f whip muxer. Only
# works for H.264 — ffmpeg 8.x explicitly rejects VP9/AV1 with "whip
# muxer supports only codec h264 for type video". Echoes the ffmpeg
# pid so callers can track it.
_lib_whip_publish_ffmpeg() {
    local color=$1 tone=$2 token=$3 logfile=$4
    local venc
    venc=$(_lib_video_encode_args) || return 1
    ffmpeg -hide_banner -loglevel warning -nostats \
        -re \
        -f lavfi -i "$color,format=yuv420p" \
        -f lavfi -i "$tone" \
        $venc \
        -c:a libopus -b:a 64k -ar 48000 -ac 2 \
        -authorization "$token" \
        -f whip "$BASE_URL/whip/$ROOM" \
        >"$logfile" 2>&1 &
    echo $!
}

# _lib_whip_publish_whipsim pushes via the pion-based whipsim tool,
# fed by an ffmpeg encoder writing IVF (video) + Ogg (audio) to two
# FIFOs. This is the only way to exercise VP9/AV1 ingest because
# ffmpeg's own WHIP muxer is H.264-only. Echoes the whipsim pid so
# callers can track it for kill/wait; the ffmpeg side is tracked via
# lib_track_pid so cleanup reaps it too.
_lib_whip_publish_whipsim() {
    local color=$1 tone=$2 token=$3 logfile=$4 label=$5 url=$6
    local venc
    venc=$(_lib_video_encode_args) || return 1
    local vfifo="$STATE_DIR/${label}.ivf"
    local afifo="$STATE_DIR/${label}.ogg"
    rm -f "$vfifo" "$afifo"
    mkfifo "$vfifo" "$afifo"

    # ffmpeg encodes into the two FIFOs. `-f ivf` carries VP8/VP9/AV1
    # frames only; whipsim consumes them as opaque sample payloads.
    # -y overrides ffmpeg's interactive overwrite prompt — the FIFO
    # already exists from the mkfifo above, and ffmpeg mistakes it
    # for an existing regular file to be clobbered.
    # Ogg/Opus needs -page_duration 20000 (= 20 ms) so each Ogg page
    # carries exactly one Opus packet; the default 1 s page packing
    # bundles ~50 packets per page and whipsim cannot split them back
    # apart, leaving the recording silent.
    ffmpeg -hide_banner -loglevel warning -nostats -y -re \
        -f lavfi -i "$color,format=yuv420p" \
        -f lavfi -i "$tone" \
        -map 0:v $venc -f ivf "$vfifo" \
        -map 1:a -c:a libopus -b:a 64k -ar 48000 -ac 2 \
        -page_duration 20000 -f ogg "$afifo" \
        >"${logfile}.ffmpeg" 2>&1 &
    local fpid=$!
    lib_track_pid "$fpid"

    "$REPO_ROOT/bin/whipsim" \
        -url "$url" \
        -token "$token" \
        -video-ivf "$vfifo" \
        -audio-ogg "$afifo" \
        -video-codec "$VIDEO_CODEC" \
        -duration 10m \
        >"$logfile" 2>&1 &
    echo $!
}

# _lib_whip_publish is the codec-aware dispatcher used by the room
# publishers (userA/userB). The mix-leg publisher in verify.sh calls
# the concrete helpers directly because it uses a different URL.
_lib_whip_publish() {
    local color=$1 tone=$2 token=$3 logfile=$4 label=$5
    case "$VIDEO_CODEC" in
        h264)
            _lib_whip_publish_ffmpeg "$color" "$tone" "$token" "$logfile"
            ;;
        vp8|vp9|av1)
            # ffmpeg's -f whip muxer is hard-coded to H.264, so VP8 / VP9
            # / AV1 ingest goes through whipsim, which talks pion WebRTC
            # directly. The pipeline is identical for all three codecs:
            # ffmpeg → IVF + Ogg FIFOs → whipsim → axonkit WHIP ingest.
            _lib_whip_publish_whipsim "$color" "$tone" "$token" "$logfile" "$label" \
                "$BASE_URL/whip/$ROOM"
            ;;
        *)
            echo "unsupported VIDEO_CODEC=$VIDEO_CODEC" >&2
            return 1
            ;;
    esac
}

lib_start_publishers() {
    echo "[publisher] userA: red 440Hz → /whip/$ROOM (codec=$VIDEO_CODEC)"
    PUB_A_PID=$(_lib_whip_publish \
        "color=red:size=320x240:rate=30" \
        "sine=frequency=440:sample_rate=48000,aresample=async=1:first_pts=0" \
        "$TOKEN_A" \
        "$LOG_DIR/publisher-userA.log" \
        "publisher-userA")
    lib_track_pid "$PUB_A_PID"

    echo "[publisher] userB: green 880Hz → /whip/$ROOM (codec=$VIDEO_CODEC)"
    PUB_B_PID=$(_lib_whip_publish \
        "color=green:size=320x240:rate=30" \
        "sine=frequency=880:sample_rate=48000,aresample=async=1:first_pts=0" \
        "$TOKEN_B" \
        "$LOG_DIR/publisher-userB.log" \
        "publisher-userB")
    lib_track_pid "$PUB_B_PID"
}

# lib_wait_settle gives the publishers a couple of seconds to negotiate
# WHIP + start pushing RTP before the script asks axonkit to build a
# broadcast around them. In practice 4 s is plenty for loopback.
lib_wait_settle() {
    sleep 4
}

# ---------------------------------------------------------------------------
# Broadcast creation + status polling.
# ---------------------------------------------------------------------------
lib_create_broadcast() {
    echo "[broadcast] create: room=$ROOM liveIdentity=userA"
    local create_body
    create_body=$(cat <<EOF
{
  "room": "$ROOM",
  "participantIdentity": "userA",
  "destinations": [
    { "type": "rtmp", "url": "rtmp://127.0.0.1:$DST_RTMP_PORT/live/out" }
  ]
}
EOF
)
    local resp
    resp=$(curl -sS -X POST "$BASE_URL/rtc/broadcast" \
        -H "Authorization: Bearer $TOKEN_ADMIN" \
        -H "Content-Type: application/json" \
        -d "$create_body")
    echo "$resp" > "$LOG_DIR/broadcast-create.json"
    BID=$(printf '%s' "$resp" | awk -F'"' '/"id"/ {print $4; exit}')
    if [[ -z "$BID" || "$BID" == *error* ]]; then
        echo "failed to create broadcast: $resp" >&2
        exit 1
    fi
    echo "[broadcast] id=$BID"
    T0=$(date +%s)
}

lib_broadcast_snapshot() {
    curl -sS "$BASE_URL/rtc/broadcast/$BID" \
        -H "Authorization: Bearer $TOKEN_ADMIN"
}

lib_broadcast_source() {
    lib_broadcast_snapshot \
        | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("source",""))' \
        2>/dev/null || true
}

# lib_wait_source <want> <timeout> — block until broadcast source is
# <want> (live/mix) or timeout elapses. Returns the elapsed seconds
# through the global T_*_FLIP/*_LATENCY/*_REC variables.
lib_wait_source() {
    local want=$1 timeout=$2
    echo "[wait-source] waiting up to ${timeout}s for switcher to reach '$want'"
    local start
    start=$(date +%s)
    local i flip=0
    for i in $(seq 1 $((timeout * 2))); do
        if [[ "$(lib_broadcast_source)" == "$want" ]]; then
            flip=$(date +%s)
            break
        fi
        sleep 0.5
    done
    if [[ "$flip" -eq 0 ]]; then
        echo "switcher never reached '$want' within ${timeout}s" >&2
        echo "final status snapshot:" >&2
        lib_broadcast_snapshot >&2
        echo >&2
        return 1
    fi
    local latency=$(( flip - start ))
    local rec=$(( flip - T0 ))
    if [[ "$want" == "mix" ]]; then
        T_MIX_FLIP=$flip
        MIX_FLIP_LATENCY=$latency
        MIX_FLIP_REC=$rec
    else
        T_LIVE_FLIP=$flip
        LIVE_FLIP_LATENCY=$latency
        LIVE_FLIP_REC=$rec
    fi
    echo "[wait-source] flipped to $want after ${latency}s (rec t≈${rec}s)"
}

# lib_delete_mix_ingest forces the switcher to detach the mix source by
# sending DELETE /rtc/broadcast/{id}/whip. Used by both variants — for
# avflow it races Chrome's teardown, for ffmpeg it short-circuits the
# publisher's own disconnect path so the wait-for-live is tight.
lib_delete_mix_ingest() {
    echo "[broadcast] DELETE /rtc/broadcast/$BID/whip (force mix detach)"
    curl -sS -X DELETE "$BASE_URL/rtc/broadcast/$BID/whip" \
        -H "Authorization: Bearer $TOKEN_ADMIN" \
        -o /dev/null -w "status=%{http_code}\n" \
        | tee "$LOG_DIR/broadcast-mix-delete.txt"
}

lib_status_snapshot_to_log() {
    local status
    status=$(lib_broadcast_snapshot)
    echo "$status" > "$LOG_DIR/broadcast-status.json"
    echo "[status] $status"
}

lib_delete_broadcast() {
    echo "[broadcast] DELETE /rtc/broadcast/$BID"
    curl -sS -X DELETE "$BASE_URL/rtc/broadcast/$BID" \
        -H "Authorization: Bearer $TOKEN_ADMIN" -o /dev/null -w "status=%{http_code}\n" \
        | tee "$LOG_DIR/broadcast-delete.txt"
}

# ---------------------------------------------------------------------------
# Recorder teardown and FLV assembly.
# ---------------------------------------------------------------------------
lib_stop_publishers_and_recorder() {
    # Guard against PID=0 (uninitialised) and PID=1 (init): bash's `kill 0`
    # signals the script's own process group, which would silently kill
    # the verify run mid-teardown. Variants that don't use the canonical
    # two-publisher fixture (e.g. verify-rtmp.sh) leave PUB_A_PID/PUB_B_PID
    # at their default 0; those skips are intentional.
    for pid in "$PUB_A_PID" "$PUB_B_PID"; do
        if [[ "$pid" -gt 1 ]]; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    sleep 2
    if [[ -f "$RECORDER_LOOP_PID_FILE" ]]; then
        local rpid
        rpid=$(cat "$RECORDER_LOOP_PID_FILE")
        kill -TERM "$rpid" 2>/dev/null || true
        pkill -TERM -P "$rpid" 2>/dev/null || true
        sleep 1
        kill -KILL "$rpid" 2>/dev/null || true
    fi
}

# lib_assemble_flv concatenates captured segments into REC_DIR/out.flv,
# falling back to the largest single segment if concat fails. Returns
# 0 and echoes FLV path on success, 1 if nothing was captured.
lib_assemble_flv() {
    shopt -s nullglob
    local segments=("$REC_DIR"/seg-*.flv)
    shopt -u nullglob
    if [[ ${#segments[@]} -eq 0 ]]; then
        echo "no RTMP segments captured — recorder never accepted a connection" >&2
        return 1
    fi
    echo "[recorder] captured ${#segments[@]} segment(s):" >&2
    local s size
    for s in "${segments[@]}"; do
        size=$(stat -f%z "$s" 2>/dev/null || stat -c%s "$s")
        echo "  $(basename "$s") ($size bytes)" >&2
    done

    local flv="$REC_DIR/out.flv"
    if [[ ${#segments[@]} -eq 1 ]]; then
        cp "${segments[0]}" "$flv"
    else
        local concat_list="$REC_DIR/concat.txt"
        : > "$concat_list"
        for s in "${segments[@]}"; do
            printf "file '%s'\n" "$s" >> "$concat_list"
        done
        ffmpeg -hide_banner -loglevel error -y \
            -f concat -safe 0 -i "$concat_list" \
            -c copy "$flv" >>"$LOG_DIR/recorder.log" 2>&1 || {
            local biggest="${segments[0]}" biggest_size=0 sz
            for s in "${segments[@]}"; do
                sz=$(stat -f%z "$s" 2>/dev/null || stat -c%s "$s")
                if (( sz > biggest_size )); then
                    biggest="$s"
                    biggest_size=$sz
                fi
            done
            cp "$biggest" "$flv"
        }
    fi
    if [[ ! -s "$flv" ]]; then
        echo "recording missing or empty at $flv" >&2
        return 1
    fi
    echo "$flv"
}

# ---------------------------------------------------------------------------
# Frame probing and colour assertions.
#
# probe_frame.py returns "R,G,B" for the mean colour of a PNG; we compare
# those triples to expected phase colours. The assertions intentionally
# use generous thresholds because the livestream→mix→livestream flip
# can smear neighbouring frames around the exact boundary.
# ---------------------------------------------------------------------------
PROBE_HELPER_PATH="$SCRIPT_DIR/probe_frame.py"

lib_split_rgb() {
    local rgb="$1"
    IFS=',' read -r __R __G __B <<<"$rgb"
    printf '%s %s %s' "$__R" "$__G" "$__B"
}

lib_check_red()   { awk -F, -v r="$1" -v g="$2" -v b="$3" 'BEGIN { exit !(r > 120 && r > g + 30 && r > b + 30) }'; }
lib_check_green() { awk -F, -v r="$1" -v g="$2" -v b="$3" 'BEGIN { exit !(g > 80  && g > r + 20 && g > b + 20) }'; }
lib_check_blue()  { awk -F, -v r="$1" -v g="$2" -v b="$3" 'BEGIN { exit !(b > 80  && b > r + 20 && b > g + 20) }'; }
lib_check_mix_mean() {
    # Red+Green composite mean: both channels ≥60, blue strictly minor.
    awk -F, -v r="$1" -v g="$2" -v b="$3" 'BEGIN {
        exit !(r > 60 && g > 60 && b < r/2 && b < g/2 && r < 200 && g < 200)
    }'
}

# lib_extract_frame <mp4> <seek_ts> <png>
lib_extract_frame() {
    local mp4=$1 ts=$2 png=$3
    ffmpeg -hide_banner -loglevel error -y -ss "$ts" -i "$mp4" \
        -frames:v 1 "$png" \
        >>"$LOG_DIR/ffmpeg-probe.log" 2>&1 || true
    [[ -s "$png" ]]
}

# lib_mean_rgb <png> -> "R,G,B"
lib_mean_rgb() {
    python3 "$PROBE_HELPER_PATH" "$1" 2>>"$LOG_DIR/ffmpeg-probe.log"
}

# lib_probe_half <mp4> <seek_ts> <left|right> <png> -> "R,G,B"
lib_probe_half() {
    local mp4=$1 ts=$2 side=$3 png=$4
    local crop
    if [[ "$side" == "left" ]]; then
        crop="crop=iw/2:ih:0:0"
    else
        crop="crop=iw/2:ih:iw/2:0"
    fi
    ffmpeg -hide_banner -loglevel error -y -ss "$ts" -i "$mp4" \
        -vf "$crop" -frames:v 1 "$png" \
        >>"$LOG_DIR/ffmpeg-probe.log" 2>&1 || true
    if [[ ! -s "$png" ]]; then
        return 1
    fi
    lib_mean_rgb "$png"
}

# ---------------------------------------------------------------------------
# lib_finalize <mix_phase_checker> [mix_halves_checker]
#
# <mix_phase_checker>  : function taking R G B that returns 0 if the
#                        mix-phase mean is "correct" for this variant.
# <mix_halves_checker> : optional. Takes L_RGB R_RGB, returns 0 if the
#                        composite halves are right for the variant.
#
# Phase 1 and 3 assertions (red-dominant) are variant-agnostic.
# ---------------------------------------------------------------------------
lib_finalize() {
    local mix_check=$1
    local halves_check=${2:-}

    local flv
    flv=$(lib_assemble_flv) || exit 1

    echo "[verify] ffprobe $flv"
    local duration v_streams a_streams
    duration=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$flv" | awk '{printf "%d", $1}')
    v_streams=$(ffprobe -v error -select_streams v -show_entries stream=codec_name -of csv=p=0 "$flv" | wc -l | tr -d ' ')
    a_streams=$(ffprobe -v error -select_streams a -show_entries stream=codec_name -of csv=p=0 "$flv" | wc -l | tr -d ' ')
    echo "[verify] duration=${duration}s video_streams=$v_streams audio_streams=$a_streams"

    local min_duration=$(( WARMUP_LIVE + MIX_FLIP_LATENCY + MIX_STABLE + LIVE_FLIP_LATENCY + POST_DETACH - 6 ))
    if [[ "$duration" -lt $min_duration ]]; then
        echo "recording duration $duration is shorter than expected switching window ($min_duration)" >&2
        exit 1
    fi
    if [[ "$v_streams" -lt 1 ]]; then
        echo "no video stream in recording" >&2
        exit 1
    fi
    if [[ "$a_streams" -lt 1 ]]; then
        echo "no audio stream in recording" >&2
        exit 1
    fi

    local stamp dest_dir mp4_path
    stamp=$(date +%Y%m%d-%H%M%S)
    # One subdirectory per run keeps MP4 + frame PNGs + report colocated
    # so a failing run can be inspected by pointing an image viewer at
    # the directory. The variant + timestamp already live in the
    # directory name, so the files inside are short and uniform:
    # recording.mp4, frames.txt, tNs-<label>.png. OUT_DIR lives under
    # test/broadcast/ and is .gitignore'd at the repo root so commits
    # don't pick up artefacts.
    dest_dir="$OUT_DIR/$VARIANT-$stamp"
    mp4_path="$dest_dir/recording.mp4"
    mkdir -p "$dest_dir"
    ffmpeg -hide_banner -loglevel error -y -i "$flv" -c copy "$mp4_path"

    local probe_ts=(
        "$(( WARMUP_LIVE / 2 ))"
        "$(( MIX_FLIP_REC + MIX_STABLE / 2 ))"
        "$(( LIVE_FLIP_REC + POST_DETACH / 2 ))"
    )
    local probe_label=(live-A mix-AB live-A-again)
    local frame_report="$dest_dir/frames.txt"
    : > "$frame_report"

    declare -a mean_rgb=()
    local i ts label png mean
    for i in 0 1 2; do
        ts="${probe_ts[$i]}"
        label="${probe_label[$i]}"
        png="$dest_dir/t${ts}s-$label.png"
        if ! lib_extract_frame "$mp4_path" "$ts" "$png"; then
            echo "failed to extract frame at ${ts}s (label=$label)" >&2
            echo "  mp4=$mp4_path" >&2
            exit 1
        fi
        if ! mean=$(lib_mean_rgb "$png"); then
            echo "probe_frame.py failed for ${png}" >&2
            exit 1
        fi
        mean_rgb[$i]="$mean"
        printf 'phase=%s ts=%ss mean_rgb=%s png=%s\n' \
            "$label" "$ts" "$mean" "$png" | tee -a "$frame_report"
    done

    local fail=0
    read -r R1 G1 B1 <<<"$(lib_split_rgb "${mean_rgb[0]}")"
    read -r R2 G2 B2 <<<"$(lib_split_rgb "${mean_rgb[1]}")"
    read -r R3 G3 B3 <<<"$(lib_split_rgb "${mean_rgb[2]}")"

    if ! lib_check_red "$R1" "$G1" "$B1"; then
        echo "assertion failed: phase-1 (live) expected red-dominant, got ${mean_rgb[0]}" >&2
        fail=1
    fi
    if ! "$mix_check" "$R2" "$G2" "$B2"; then
        echo "assertion failed: phase-2 (mix) got mean=${mean_rgb[1]}" >&2
        fail=1
    fi
    if ! lib_check_red "$R3" "$G3" "$B3"; then
        echo "assertion failed: phase-3 (fallback) expected red-dominant, got ${mean_rgb[2]}" >&2
        fail=1
    fi

    if [[ -n "$halves_check" ]]; then
        local mix_ts="${probe_ts[1]}"
        local l_png="$dest_dir/t${mix_ts}s-mix-left.png"
        local r_png="$dest_dir/t${mix_ts}s-mix-right.png"
        local l_rgb r_rgb
        l_rgb=$(lib_probe_half "$mp4_path" "$mix_ts" left  "$l_png") || { echo "failed to probe left half" >&2; fail=1; }
        r_rgb=$(lib_probe_half "$mp4_path" "$mix_ts" right "$r_png") || { echo "failed to probe right half" >&2; fail=1; }
        if [[ -n "$l_rgb" ]]; then
            printf 'mix-left  ts=%ss mean_rgb=%s png=%s\n' "$mix_ts" "$l_rgb" "$l_png" | tee -a "$frame_report"
        fi
        if [[ -n "$r_rgb" ]]; then
            printf 'mix-right ts=%ss mean_rgb=%s png=%s\n' "$mix_ts" "$r_rgb" "$r_png" | tee -a "$frame_report"
        fi
        if [[ -n "$l_rgb" && -n "$r_rgb" ]]; then
            if ! "$halves_check" "$l_rgb" "$r_rgb"; then
                echo "assertion failed: composite halves mismatch (left=$l_rgb right=$r_rgb)" >&2
                fail=1
            fi
        fi
    fi

    if [[ $fail -ne 0 ]]; then
        echo "frame colour assertions FAILED (recording saved to $mp4_path for manual review)" >&2
        exit 1
    fi

    echo ""
    echo "=============================================================="
    echo " verify-broadcast[$VARIANT]: OK"
    echo "   codec     : $VIDEO_CODEC"
    echo "   recording : $mp4_path"
    echo "   frames    : $frame_report"
    echo "   logs      : $LOG_DIR"
    echo "=============================================================="
}
