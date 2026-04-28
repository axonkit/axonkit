#!/usr/bin/env bash
# End-to-end verification of the RTMP-ingest → broadcast → RTMP-egress
# pipeline, exercising a mid-stream WHIP "mix" overlay to simulate a
# live-↔-mix-↔-live source flip on top of the same RTMP recording.
# Push an H.264 (with B frames) + Opus stream into axonkit over RTMP,
# create a /rtc/broadcast targeting that participant, then partway
# through the run inject a WHIP publisher into /rtc/broadcast/{id}/whip
# so the broadcast switcher flips live→mix→live, and verify the
# captured RTMP recording remains playable end-to-end.
#
# What this exercises that verify.sh does not:
#   - axonkit's go-rtmp ingest (-rtmp-listen) and FLV demuxer
#   - the H.264 depacketizer's CTS handling on classic-FLV B frames
#     (verify.sh forces -bf 0 because WHIP forbids B frames)
#   - the Enhanced-RTMP "Opus" FourCC audio path on both ingest and
#     egress (ffmpeg writes E-RTMP audio when -c:a libopus + -f flv)
#   - mid-stream live↔mix flips while a B-frame-heavy RTMP source is
#     publishing — the previous version of this script only ran a
#     pure RTMP-only recording, missing the source-switcher path
#
# Pipeline:
#   1. Build axonkit + token + whipsim CLIs.
#   2. Start axonkit on loopback with RTMP ingest enabled (its embedded
#      NATS server boots automatically; no external broker).
#   3. Start a local ffmpeg RTMP listener that records what arrives at
#      rtmp://127.0.0.1:$DST_RTMP_PORT/live/out into FLV segments. Each
#      mid-stream codec change (live↔mix) drops the recorder's TCP
#      socket and starts a new segment; lib_assemble_flv concatenates
#      them at the end of the run.
#   4. ffmpeg pushes H.264 (libx264, -bf 3, high profile, B frames
#      enabled) + Opus to rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/<room>?
#      token=<jwt>. axonkit registers the publisher as participant
#      "rtmpUser" with one Opus and one H.264 producer.
#   5. POST /rtc/broadcast with participantIdentity=rtmpUser and one
#      RTMP destination pointing at the recorder. The broadcast pulls
#      the participant's tracks via mediasoup consumers, depacketizes,
#      and pushes them out as FLV.
#   6. Phase A ($LIVE_BEFORE_MIX s): live-only — recording captures the
#      RTMP B-frame stream.
#   7. Phase B (~$MIX_STABLE s): inject a WHIP publisher into
#      /rtc/broadcast/{id}/whip. Two codec routes:
#        - $MIX_CODEC=h264 → ffmpeg's own -f whip muxer (the only
#          codec it supports natively).
#        - $MIX_CODEC=h265 → whipsim, fed raw HEVC via a FIFO. ffmpeg
#          rejects H.265 in both -f whip and -f ivf, so we round-trip
#          Annex-B through pion in cmd/whipsim.
#      The broadcast switcher's source flips to "mix"; the RTMP
#      recorder usually drops its TCP socket because the mix codec's
#      SPS/PPS (or FourCC, for H.265) differs from the RTMP leg. A new
#      FLV segment opens for the mix phase.
#   8. Phase C ($POST_MIX_LIVE s): stop the mix publisher, DELETE the
#      mix ingest, source flips back to "live"; another segment opens
#      for the post-mix RTMP recording.
#   9. Stop ffmpeg, DELETE the broadcast, stop the recorder.
#  10. Assemble captured FLV segments → MP4, run ffmpeg null decode and
#      ffprobe to confirm both streams are present and the recording
#      decodes. The strict B-frame / monotonic-DTS / PTS!=DTS asserts
#      are scoped to the *first* segment (the pure RTMP-only window):
#      mid-stream codec flips can legitimately introduce timestamp
#      restarts at segment boundaries.
#
# Flip latency expectations (logged by [verify-flips], not asserted):
#   - live → mix: visible "frozen frame" of the live publisher across
#     the flip. Magnitude = the live RTMP ingest's end-to-end encoder
#     buildup (libx264 -bf 3 -preset veryfast accumulates ~1.5 s of
#     B-pyramid + rate-control buffer; -tune zerolatency -bf 0 reduces
#     it to ~0.5 s). The broadcast switcher anchors the new mix's
#     first video frame on the audio rebaser's high-water mark so
#     mix audio and mix video start synchronised, which is what
#     surfaces the residual ingest lag as a video gap rather than as
#     A/V drift in the recording.
#   - mix → live: pusher waits for the next live IDR before emitting
#     anything (a P-frame sent into a fresh SeqHeader context cannot be
#     decoded, so dropping until the next IDR is the only correct
#     behaviour). Magnitude = up to one live GOP — ~2 s with the
#     default `-g 60`, ~1 s with `-g 30`. RTMP has no PLI back-channel
#     so axonkit cannot force the upstream encoder to emit an early
#     IDR; the gap is bounded entirely by the ingest's GOP setting.
# Set RTMP_LOW_LATENCY=1 to swap the publisher to a short-GOP no-B
# profile and quantify the improvement against the default config.

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

# Phase durations. Total RTMP push duration is the sum + a small
# buffer so ffmpeg's RTMP publisher outlives the mix-flip → mix-detach
# round-trip and Phase C never starves on data.
: "${LIVE_BEFORE_MIX:=5}"
: "${MIX_STABLE:=10}"
: "${POST_MIX_LIVE:=5}"
: "${MIX_FLIP_TIMEOUT:=20}"
: "${LIVE_FLIP_TIMEOUT:=15}"

# Mix overlay codec: h264 (default — pushed via ffmpeg's -f whip) or
# h265 (pushed via whipsim, since ffmpeg's WHIP muxer is H.264 only).
: "${MIX_CODEC:=h264}"
MIX_CODEC="$(printf '%s' "$MIX_CODEC" | tr '[:upper:]' '[:lower:]')"
case "$MIX_CODEC" in
    h264|h265) ;;
    *)
        echo "unsupported MIX_CODEC=$MIX_CODEC (want h264 or h265)" >&2
        exit 1
        ;;
esac

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
# Mint an admin token for the WHIP mix overlay. The broadcast mix
# ingest endpoint requires admin scope (see /rtc/broadcast/{id}/whip
# routes); we reuse the same admin minted for the broadcast create
# call to keep the trust boundary uniform across the run.
TOKEN_MIX_PUB="$TOKEN_ADMIN"

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
# Total stream length is sized to outlive Phase A + Phase B + Phase C.
# ---------------------------------------------------------------------------
PUSH_URL="rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/${ROOM}?token=${RTMP_TOKEN}"
PUSH_LOG="$LOG_DIR/rtmp-push.log"
RTMP_DURATION=$(( LIVE_BEFORE_MIX + MIX_FLIP_TIMEOUT + MIX_STABLE + LIVE_FLIP_TIMEOUT + POST_MIX_LIVE + 5 ))
echo "[rtmp-push] starting H.264(B-frames)+Opus testsrc → rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/$ROOM (identity=$RTMP_IDENTITY, duration=${RTMP_DURATION}s)"
# Source: lavfi testsrc = colour bars + a per-frame counter and a
# scrolling indicator. Real inter-frame motion makes libx264 emit a
# representative I/P/B mix (a static colour would flatten to ~all
# skip-blocks and barely exercise the B-frame path) and the frame
# counter doubles as a visual chronometer when reviewing the recorded
# MP4 by eye.
# Encoder profile selection: by default we exercise the full B-frame
# path (high profile, -bf 3, -g 60). RTMP_LOW_LATENCY=1 swaps in a
# zero-latency profile (no B-frames, -g 30) — the same profile real
# OBS / hardware-encoder publishers use when chasing sub-second
# end-to-end latency. We keep this knob in the verify script so a
# developer chasing the live↔mix switch latency can A/B the encoder
# config without forking the script: with RTMP_LOW_LATENCY=1 the
# live ingest no longer accumulates ~1.5 s of B-pyramid + RC buildup
# and the mix↔live flip lands within a single GOP (~1 s instead of
# ~2-3 s), which is what the post-flip "frozen video" / "delayed
# video" symptoms in the recording boil down to.
: "${RTMP_LOW_LATENCY:=0}"
if [[ "$RTMP_LOW_LATENCY" == "1" ]]; then
    VIDEO_ENC_OPTS=(-c:v libx264 -preset ultrafast -tune zerolatency
        -profile:v main -bf 0 -g 30 -keyint_min 30 -r 30
        -pix_fmt yuv420p -b:v 800k)
    echo "[rtmp-push] RTMP_LOW_LATENCY=1: ingest configured for sub-second flip recovery"
else
    VIDEO_ENC_OPTS=(-c:v libx264 -preset veryfast -tune zerolatency -profile:v high -bf 3
        -b_strategy 1 -g 60 -r 30 -pix_fmt yuv420p -b:v 800k)
fi
ffmpeg -hide_banner -loglevel warning -nostats \
    -re \
    -t "$RTMP_DURATION" \
    -f lavfi -i "testsrc=size=640x480:rate=30,format=yuv420p" \
    -f lavfi -i "sine=frequency=440:sample_rate=48000,aresample=async=1:first_pts=0" \
    "${VIDEO_ENC_OPTS[@]}" \
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

# ---------------------------------------------------------------------------
# Phase A: live-only recording window. The first FLV segment captured
# from now until the mix flip is the one we apply the strict B-frame
# / monotonic-DTS asserts to (Phase A frames are pure RTMP B-frame
# output — no intervening source flip).
# ---------------------------------------------------------------------------
echo "[phase-A] live-only ($LIVE_BEFORE_MIX s) → recording captures RTMP B-frame stream"
sleep "$LIVE_BEFORE_MIX"

# ---------------------------------------------------------------------------
# Phase B: mix overlay. Push a WHIP publisher into
# /rtc/broadcast/{id}/whip. The codec path differs:
#   - h264 → ffmpeg's -f whip muxer (the only WHIP codec ffmpeg ships).
#   - h265 → whipsim, with raw HEVC fed via a FIFO (ffmpeg can't WHIP
#     non-H.264 codecs and its IVF muxer refuses H.265).
#
# We deliberately don't try to make the mix codec match the RTMP leg's
# SPS/PPS byte-for-byte: -bf 0 vs -bf 3 plus profile differences mean
# axonkit will emit a fresh AVCDecoderConfigurationRecord on the flip,
# the recorder ffmpeg drops its TCP connection, and a new FLV segment
# opens. lib_assemble_flv concatenates the segments at the end.
# ---------------------------------------------------------------------------
MIX_URL="$BASE_URL/rtc/broadcast/$BID/whip"
MIX_LOG="$LOG_DIR/mix-whip.log"
MIX_VIDEO_SRC="color=blue:size=640x480:rate=30"
MIX_AUDIO_SRC="sine=frequency=1000:sample_rate=48000,aresample=async=1:first_pts=0"

echo "[mix-whip] codec=$MIX_CODEC starting → $MIX_URL"
case "$MIX_CODEC" in
    h264)
        # ffmpeg WHIP muxer: requires -bf 0, accepts only H.264. The
        # encode args are intentionally explicit so this script
        # remains independent of $VIDEO_CODEC (which lib.sh sets up
        # for the WHIP-room publishers — which verify-rtmp.sh does
        # not use).
        ffmpeg -hide_banner -loglevel warning -nostats \
            -re \
            -f lavfi -i "$MIX_VIDEO_SRC,format=yuv420p" \
            -f lavfi -i "$MIX_AUDIO_SRC" \
            -c:v libx264 -preset ultrafast -tune zerolatency -profile:v main -bf 0 \
            -pix_fmt yuv420p -g 30 -r 30 \
            -c:a libopus -b:a 64k -ar 48000 -ac 2 \
            -authorization "$TOKEN_MIX_PUB" \
            -f whip "$MIX_URL" \
            >"$MIX_LOG" 2>&1 &
        MIX_PID=$!
        ;;
    h265)
        # whipsim path: lib's helper expects $VIDEO_CODEC to drive the
        # encoder choice. The RTMP leg above doesn't read $VIDEO_CODEC,
        # so flipping it locally is safe. We restore it after the
        # helper returns to keep the interface honest for any future
        # caller in the same run.
        _saved_codec="$VIDEO_CODEC"
        VIDEO_CODEC="h265"
        MIX_PID=$(_lib_whip_publish_whipsim_h265 \
            "$MIX_VIDEO_SRC" \
            "$MIX_AUDIO_SRC" \
            "$TOKEN_MIX_PUB" \
            "$MIX_LOG" \
            "mix-whip" \
            "$MIX_URL")
        VIDEO_CODEC="$_saved_codec"
        ;;
esac
lib_track_pid "$MIX_PID"

if ! lib_wait_source "mix" "$MIX_FLIP_TIMEOUT"; then
    echo "mix publisher log tail:" >&2
    tail -40 "$MIX_LOG" >&2
    exit 1
fi

echo "[phase-B] mix active ($MIX_STABLE s) → recording covers the live↔mix flip"
sleep "$MIX_STABLE"

echo "[mix-whip] stopping publisher pid=$MIX_PID"
kill -TERM "$MIX_PID" 2>/dev/null || true
# Short grace for the publisher's own DELETE, then short-circuit any
# stragglers via the mix-ingest endpoint so the live flip is tight.
sleep 1
lib_delete_mix_ingest

if ! lib_wait_source "live" "$LIVE_FLIP_TIMEOUT"; then
    echo "broadcast never returned to source=live after mix detach" >&2
    exit 1
fi

echo "[phase-C] live-resumed ($POST_MIX_LIVE s) → recording captures RTMP again"
sleep "$POST_MIX_LIVE"

# ---------------------------------------------------------------------------
# Teardown. Order matters:
#   1. Snapshot status while the broadcast is still alive (so the log
#      captures source/pushers in their final post-Phase-C state).
#   2. DELETE the broadcast — this is the clean shutdown path:
#      Broadcast.terminate stops the switcher, closes mediasoup
#      consumers, and stops the pushers, which lets the recorder
#      ffmpeg see a proper FIN on its TCP socket and flush the
#      trailing FLV tags rather than truncating mid-tag.
#   3. THEN kill the RTMP publisher. We deliberately do this AFTER
#      the broadcast DELETE so the auto-cleanup path does not race
#      us — if we killed ffmpeg first, axonkit's RTMP ingress
#      would notice the producer disappear and self-terminate the
#      broadcast with reason=live_identity_gone (see
#      newLiveSource OnProducerGone hook). The explicit DELETE
#      that followed would then return 404 ("broadcast: not
#      found"), which is correct semantics but noisy in the log
#      and masks any real DELETE failure.
# ---------------------------------------------------------------------------
lib_status_snapshot_to_log
lib_delete_broadcast
sleep 1
echo "[rtmp-push] stopping ffmpeg pid=$RTMP_PUSH_PID"
kill -TERM "$RTMP_PUSH_PID" 2>/dev/null || true
sleep 1
lib_stop_publishers_and_recorder

# ---------------------------------------------------------------------------
# Verify the recording.
#
# Why per-segment instead of one merged MP4: each codec class change
# (live H.264 ↔ mix H.265, or any future classic-AVC ↔ E-RTMP-FourCC
# flip) makes axonkit's RTMP pusher force a TCP reconnect, which
# rotates the recorder's FLV segment file. That delivers each codec
# in its own clean single-codec FLV (seg-001=h264, seg-002=hevc for
# MIX_CODEC=h265, seg-003=h264, …). MP4 cannot represent multiple
# codecs in a single track, and `ffmpeg -f concat -c copy` on the
# mixed-codec FLV list silently re-labels every tag as the first
# segment's codec — producing a recording.mp4 that looks h264-only
# even when seg-002.flv is full of hevc frames. The right answer is
# to validate each segment standalone:
#   - segment 1 (Phase A live H.264) gets the full strict gate:
#     B-frame presence, monotonic DTS, PTS!=DTS, decode pass.
#   - middle segments (mix overlay) get a codec match against
#     $MIX_CODEC, decode pass, and a duration floor.
#   - the final live segment (Phase C) gets a codec match (h264) and
#     a decode pass.
# Aggregate duration / segment count are checked across the set.
#
# An assembled FLV is still produced (lib_assemble_flv) for manual
# review and for the flip-latency report below — which is bytes-only
# and tolerant of the concat re-mux's codec confusion as long as the
# tag boundaries survive.
# ---------------------------------------------------------------------------
flv=$(lib_assemble_flv) || exit 1

shopt -s nullglob
SEGMENTS=( "$REC_DIR"/seg-*.flv )
shopt -u nullglob
SEG_COUNT=${#SEGMENTS[@]}
if [[ $SEG_COUNT -lt 1 ]]; then
    echo "FAIL: no FLV segments captured" >&2
    exit 1
fi
FIRST_SEG="${SEGMENTS[0]}"
echo "[verify] segments captured: $SEG_COUNT (first=$(basename "$FIRST_SEG"))"

stamp=$(date +%Y%m%d-%H%M%S)
dest_dir="$OUT_DIR/$VARIANT-$stamp"
mkdir -p "$dest_dir"

# Map MIX_CODEC env to ffprobe's stream.codec_name reporting. ffmpeg
# normalises H.265 / HEVC into the codec name "hevc" regardless of
# how the FLV layer wrapped it (E-RTMP FourCC=hvc1) — so a literal
# "h265" string never appears in ffprobe output and an exact-match
# would always FAIL. The other codecs ride through unchanged.
MIX_EXPECTED_CODEC="$MIX_CODEC"
case "$MIX_CODEC" in
    h265) MIX_EXPECTED_CODEC="hevc" ;;
esac

fail=0
total_duration=0
total_frames=0
phase_a_codec=""
phase_a_mp4=""
phase_a_has_b=""
phase_c_codec=""
mix_segment_seen=0

# ---------------------------------------------------------------------------
# Per-segment loop. For each captured FLV segment, transcode to MP4
# (single codec → -c copy is safe), probe codec/duration/frame count,
# and run a full decode pass. Classify the segment as "live" (h264) or
# "mix" ($MIX_EXPECTED_CODEC) by codec; the segment ordering combined
# with the source-switcher's behaviour (live → mix → live) tells us
# which phase a segment belongs to.
# ---------------------------------------------------------------------------
seg_idx=0
for seg in "${SEGMENTS[@]}"; do
    seg_idx=$((seg_idx + 1))
    seg_base=$(basename "$seg" .flv)
    seg_mp4="$dest_dir/${seg_base}.mp4"
    seg_decode_log="$dest_dir/${seg_base}-decode.log"

    ffmpeg -hide_banner -loglevel error -y -i "$seg" -c copy "$seg_mp4"

    seg_v_codec=$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "$seg_mp4")
    seg_a_codec=$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name -of csv=p=0 "$seg_mp4")
    seg_has_b=$(ffprobe -v error -select_streams v:0 -show_entries stream=has_b_frames -of csv=p=0 "$seg_mp4")
    seg_duration=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$seg_mp4" \
        | awk '{printf "%d", $1+0.5}')
    seg_frames=$(ffprobe -v error -count_frames -select_streams v:0 -show_entries stream=nb_read_frames -of csv=p=0 "$seg_mp4" 2>/dev/null || echo 0)
    seg_size=$(stat -f%z "$seg" 2>/dev/null || stat -c%s "$seg")

    total_duration=$(( total_duration + seg_duration ))
    total_frames=$(( total_frames + seg_frames ))

    # Classify: any segment whose codec matches MIX_EXPECTED_CODEC and
    # is NOT segment 1 is a mix segment. Segment 1 is always Phase A
    # (live), even if MIX_CODEC=h264 makes its codec coincide with
    # the mix codec — by construction the script only flips to mix
    # AFTER LIVE_BEFORE_MIX seconds elapse on segment 1.
    seg_role="live"
    if [[ $seg_idx -gt 1 && "$seg_v_codec" == "$MIX_EXPECTED_CODEC" && "$MIX_EXPECTED_CODEC" != "h264" ]]; then
        seg_role="mix"
        mix_segment_seen=1
    elif [[ $seg_idx -gt 1 && "$MIX_EXPECTED_CODEC" == "h264" ]]; then
        # Same-codec MIX_CODEC=h264 case: any segment past the first
        # is presumed to straddle the mix phase, but classifying it
        # as "live" or "mix" by codec alone is impossible. We tag
        # those segments "live-or-mix" and skip the strict role
        # assertions; the codec-class reconnect path doesn't fire in
        # this case anyway, so SEG_COUNT will usually be 1.
        seg_role="live-or-mix"
    fi

    echo "[verify] $seg_base: role=$seg_role codec=$seg_v_codec audio=$seg_a_codec duration=${seg_duration}s frames=$seg_frames size=${seg_size}B has_b_frames=$seg_has_b"

    # Audio codec assertion is uniform across roles: every segment
    # carries Opus (the broadcast pusher only speaks Opus on the
    # audio leg).
    if [[ "$seg_a_codec" != "opus" ]]; then
        echo "FAIL: $seg_base audio codec=$seg_a_codec (want opus)" >&2
        fail=1
    fi

    # Per-segment decode pass: each segment is single-codec, so any
    # decoder error here is a real bitstream regression rather than a
    # mid-stream codec-config artefact.
    #
    # `-f md5 -` is used (instead of `-f null -`) deliberately. The
    # null muxer auto-picks an output time_base from the stream's
    # average frame rate (here 30 fps → tbn=1/30 = 33.3 ms ticks).
    # The B-frame DTS-reorder path emits short clusters of consecutive
    # access units spaced exactly 1 ms apart (the dtsReorder guard
    # nudge — see dts_reorder.go's dtsReorderGuardTicks rationale).
    # Up to four such 1-ms-spaced FLV tags can land in a single 33 ms
    # output tick after the rounding, at which point the null muxer
    # spams "Application provided invalid, non monotonically
    # increasing dts" warnings — purely a coarse-tbn rounding side
    # effect with zero bitstream meaning. md5 hashes the decoded
    # frames straight to stdout, exercises the full decoder pipeline
    # exactly the same way, and bypasses the muxer's DTS check
    # entirely. The MD5 itself is written to /dev/null since we only
    # care about the stderr error stream.
    ffmpeg -hide_banner -v error -i "$seg_mp4" -map 0:v:0 -an -f md5 - \
        >/dev/null 2>"$seg_decode_log" || true
    if [[ -s "$seg_decode_log" ]]; then
        echo "FAIL: $seg_base decoder errors:" >&2
        sed 's/^/    /' "$seg_decode_log" >&2 | head -n 20 >&2
        fail=1
    fi

    if [[ "$seg_frames" -lt 1 ]]; then
        echo "FAIL: $seg_base decoded zero frames" >&2
        fail=1
    fi

    # Capture role-specific facts for the post-loop assertions.
    if [[ $seg_idx -eq 1 ]]; then
        phase_a_codec="$seg_v_codec"
        phase_a_mp4="$seg_mp4"
        phase_a_has_b="$seg_has_b"
    fi
    if [[ $seg_idx -eq $SEG_COUNT && "$seg_role" == "live" && $SEG_COUNT -gt 1 ]]; then
        phase_c_codec="$seg_v_codec"
    fi
done

# ---------------------------------------------------------------------------
# Aggregate-duration check. Phase boundaries each cost some recorder
# reconnect time; the floor matches the previous test's tolerance.
# ---------------------------------------------------------------------------
expected_duration=$(( LIVE_BEFORE_MIX + MIX_STABLE + POST_MIX_LIVE ))
min_duration=$(( expected_duration - 5 ))
echo "[verify] aggregate duration=${total_duration}s frames=$total_frames (expected >= ${min_duration}s)"
if [[ "$total_duration" -lt $min_duration ]]; then
    echo "FAIL: aggregate duration=$total_duration s shorter than expected (>= $min_duration s)" >&2
    fail=1
fi

# ---------------------------------------------------------------------------
# Mix-segment presence: with MIX_CODEC != h264 a successful run MUST
# show at least one mix-codec segment. Without one, the live↔mix flip
# either never reached the recorder (broadcast pusher bug) or the
# recorder stayed connected and silently dropped the mix tags (the
# pre-fix bug we are guarding against). MIX_CODEC=h264 is exempt
# because the codec class never changes and the reconnect path is
# intentionally inert.
# ---------------------------------------------------------------------------
if [[ "$MIX_EXPECTED_CODEC" != "h264" ]]; then
    if [[ $mix_segment_seen -ne 1 ]]; then
        echo "FAIL: no segment with mix codec=$MIX_EXPECTED_CODEC found; mix flip may not have reached the recorder" >&2
        fail=1
    fi
    # Phase A (segment 1) must be h264 regardless of mix codec — the
    # live publisher always pushes H.264.
    if [[ "$phase_a_codec" != "h264" ]]; then
        echo "FAIL: segment-1 video codec=$phase_a_codec (want h264 — Phase A is always live)" >&2
        fail=1
    fi
    # Phase C (the LAST segment when SEG_COUNT > 1) must be a live
    # h264 segment, since the script always detaches the mix and
    # waits for the source to flip back to live before tearing down.
    if [[ $SEG_COUNT -gt 1 && "$phase_c_codec" != "h264" ]]; then
        echo "FAIL: final segment video codec=${phase_c_codec:-<missing>} (want h264 — Phase C should be live again)" >&2
        fail=1
    fi
fi

# ---------------------------------------------------------------------------
# Phase A strict asserts: scope the B-frame / monotonic-DTS / PTS!=DTS
# checks to segment 1 alone. This is the pure RTMP-only window, the
# same gate the previous (mix-less) verify-rtmp.sh applied. We already
# ran a full decode pass on the segment in the loop above; here we
# layer on the timestamp / has_b_frames structural checks.
# ---------------------------------------------------------------------------
echo "[verify-segA] codec=$phase_a_codec has_b_frames=$phase_a_has_b"
# B-frame presence is only required in the default (high-profile, -bf 3)
# encoder profile. Under RTMP_LOW_LATENCY=1 the publisher uses
# -tune zerolatency -bf 0 specifically because the goal is sub-second
# live↔mix flip recovery rather than exercising the B-frame path; in
# that mode has_b_frames=0 is the expected outcome and the assertion
# would be a category error.
if [[ "${phase_a_has_b:-0}" == "0" && "$RTMP_LOW_LATENCY" != "1" ]]; then
    echo "FAIL: segment-1 ffprobe reports has_b_frames=0; B-frame path was not exercised" >&2
    fail=1
fi

# Strict decode pass scoped to the Phase A window of segment 1. A
# non-empty decode log here points to a real B-frame / reorder
# regression in the RTMP egress path. (The full per-segment decode
# pass above already covers the rest of segment 1.)
#
# Same `-f md5 -` reasoning as the per-segment loop above: it fully
# decodes every frame but skips the null muxer's coarse-tbn DTS
# monotonicity check that would otherwise cry wolf on the 1 ms
# dtsReorder guard nudges.
phase_a_decode_log="$dest_dir/seg-001-phaseA-decode.log"
ffmpeg -hide_banner -v error -t "$LIVE_BEFORE_MIX" -i "$phase_a_mp4" \
    -map 0:v:0 -an -f md5 - >/dev/null 2>"$phase_a_decode_log" || true
if [[ -s "$phase_a_decode_log" ]]; then
    echo "FAIL: segment-1 (Phase A) decoder errors:" >&2
    sed 's/^/    /' "$phase_a_decode_log" >&2
    fail=1
fi

phase_a_ts_csv="$dest_dir/seg-001-ts.csv"
ffprobe -v error -select_streams v:0 -show_entries packet=pts,dts -of csv=p=0 "$phase_a_mp4" >"$phase_a_ts_csv" 2>/dev/null || true
phase_a_ts_total=$(wc -l <"$phase_a_ts_csv" | tr -d ' ')
phase_a_ts_diff=$(awk -F, 'NF>=2 && $1 != $2 {n++} END {print n+0}' "$phase_a_ts_csv")
phase_a_ts_nonmono=$(awk -F, 'NF>=2 {if (NR>1 && $2+0 <= prev) n++; prev=$2+0} END {print n+0}' "$phase_a_ts_csv")
echo "[verify-segA] timestamps: packets=$phase_a_ts_total pts!=dts=$phase_a_ts_diff non-monotonic-dts=$phase_a_ts_nonmono"
if [[ "${phase_a_has_b:-0}" != "0" && "$phase_a_ts_diff" -lt 1 ]]; then
    echo "FAIL: segment-1 has_b_frames!=0 but every packet has PTS == DTS" >&2
    echo "       (egress dropped CompositionTime reorder; players would show frames in decode order)" >&2
    fail=1
fi
if [[ "$phase_a_ts_nonmono" -gt 0 ]]; then
    echo "FAIL: segment-1 has $phase_a_ts_nonmono packets with non-monotonic DTS" >&2
    fail=1
fi

# ---------------------------------------------------------------------------
# Flip-latency report: still useful as a perceived "video froze for
# X ms" metric, but the source data is now per-segment because the
# concat'd FLV's mid-stream codec changes are scrambled by ffmpeg's
# concat demuxer. The wall-clock proxy is:
#
#   gap_live_to_mix ≈ (last DTS of segment N) - 0  for segment N+1
#                     measured against the wall-clock interval the
#                     switcher reported (logged earlier as
#                     [wait-source]).
#
# We surface the wait-source latencies plus the per-segment durations
# so a developer chasing the live↔mix freeze duration has the data
# they need without needing the byte-level analyser to handle codec
# transitions across segment boundaries.
# ---------------------------------------------------------------------------
echo "[verify-flips] mix flip wait latency = ${MIX_FLIP_LATENCY:-?}s, live flip wait latency = ${LIVE_FLIP_LATENCY:-?}s"
echo "[verify-flips] segment durations: $(for s in "${SEGMENTS[@]}"; do
    d=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$s" 2>/dev/null | awk '{printf "%.2f", $1+0}')
    printf '%s=%ss ' "$(basename "$s" .flv)" "$d"
done)"

# ---------------------------------------------------------------------------
# Sanity: with MIX_CODEC != h264 we expect SEG_COUNT >= 3 (live+mix+
# live). A lower count means either the mix flip didn't actually fire
# or the reconnect path silently dropped a sentinel — both regress the
# fix, so they're hard fails. With MIX_CODEC=h264 the recorder stays
# on a single segment and SEG_COUNT=1 is the normal case.
# ---------------------------------------------------------------------------
if [[ "$MIX_EXPECTED_CODEC" != "h264" && $SEG_COUNT -lt 3 ]]; then
    echo "FAIL: SEG_COUNT=$SEG_COUNT (want >= 3 for MIX_CODEC=$MIX_CODEC; live, mix, live)" >&2
    fail=1
fi
if [[ "$MIX_EXPECTED_CODEC" == "h264" && $SEG_COUNT -lt 1 ]]; then
    echo "FAIL: no segments captured" >&2
    fail=1
fi

if [[ $fail -ne 0 ]]; then
    echo "verify-rtmp: FAILED (per-segment artefacts saved to $dest_dir for manual review)" >&2
    exit 1
fi

# Compose the per-segment summary line for the OK banner.
seg_summary=$(for s in "${SEGMENTS[@]}"; do
    seg_base=$(basename "$s" .flv)
    seg_mp4="$dest_dir/${seg_base}.mp4"
    if [[ -s "$seg_mp4" ]]; then
        c=$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "$seg_mp4")
        d=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$seg_mp4" | awk '{printf "%.1f", $1+0}')
        printf '%s(%s,%ss) ' "$seg_base" "$c" "$d"
    fi
done)

cat <<EOF

==============================================================
 verify-broadcast[rtmp]: OK
   ingest    : rtmp://127.0.0.1:$RTMP_PUSH_PORT/live/$ROOM (h264+opus, b-frames)
   mix       : $MIX_URL (codec=$MIX_CODEC)
   segments  : $seg_summary
   total     : ${total_duration}s, ${total_frames} video frames across $SEG_COUNT segment(s)
   artefacts : $dest_dir
   logs      : $LOG_DIR
==============================================================
EOF
