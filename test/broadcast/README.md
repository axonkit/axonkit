# Broadcast verify scripts

End-to-end smoke tests for the `/rtc/broadcast` feature of `axonkit`.
Each script spins up a loopback `axonkit` (which boots its own
in-process NATS + JetStream — no external broker needed), publishes
two fake room participants, drives a broadcast through the
`live → mix → live` flip, and asserts the recorded FLV on disk.

The two variants exist because the mix leg is the part most likely to
break in two very different ways — encoder / WHIP signalling issues
are caught by `verify.sh`, avflow-layout / Chrome regressions are
caught by `verify-avflow.sh`.

## Targets

| Target                         | Mix source                                                | External deps                           |
| ------------------------------ | --------------------------------------------------------- | --------------------------------------- |
| `make verify-broadcast`        | `ffmpeg -f whip` (H.264) / `whipsim` (VP9, AV1)           | none beyond ffmpeg + go                 |
| `make verify-broadcast-avflow` | real `avflow` pipeline (headless Chrome + `WHIPPushSink`) | `avflow` checkout + prebuilt web bundle |

Both call the same library (`lib.sh`) and accept the same env
variables; only the "what produces the mix stream" differs.

## Prerequisites

All variants:

- `ffmpeg` / `ffprobe` ≥ 7.1 on `$PATH`
- `curl`, `openssl`, `awk`, `python3`
- `go` toolchain (`axonkit`, `token`, `whipsim` binaries are built into
  `bin/` on every run so the tests can never lag the source tree)

There is no separate `nats-server` dependency — axonkit launches the
embedded NATS server when `-nats=""` (the default used by these
scripts), with its JetStream store pinned to the per-run `STATE_DIR`
so nothing leaks into the repo.

`verify-broadcast-avflow` additionally needs:

- A sibling `avflow/` checkout (or `AVFLOW_REPO=...`) with its web
  bundle already built: `cd $AVFLOW_REPO/web && pnpm build`
- A working headless Chrome (managed by avflow's launcher)

## Running

```bash
# Default: ffmpeg WHIP simulator, H.264
make verify-broadcast

# VP9 or AV1 (Enhanced-RTMP egress path)
VIDEO_CODEC=vp9 make verify-broadcast
VIDEO_CODEC=av1 make verify-broadcast

# Real avflow pipeline (H.264 only — WHIPPushSink pins H.264 today)
make verify-broadcast-avflow
```

> Note: `VIDEO_CODEC=vp8` is recognised by the publisher path (whipsim
> publishes VP8 ingest into the SFU) but `verify.sh` creates RTMP-only
> destinations, so VP8 will deliberately fail at `Broadcast.Start` with
> `broadcast: VP8 cannot be pushed to RTMP destinations`. The rejection
> is exercised by `pkg/broadcast.TestValidateDestinationsForCodec`.
>
> H.265 ingest does **not** flow through `verify.sh` — `ffmpeg -f whip`
> only speaks H.264 — but the codec is supported end-to-end by the
> `attachMixH265Bypass` path (avflow → axonkit pion bypass → E-RTMP
> `hvc1`). Coverage lives in `pkg/broadcast.TestH265MixSourceEndToEnd`,
> which drives a pion publisher posting an H.265 SDP offer and asserts
> the depacketized samples + raw RTP both reach the switcher pipeline.

A successful run ends with:

```
==============================================================
 verify-broadcast[ffmpeg]: OK
   codec     : vp9
   recording : /.../test/broadcast/out/ffmpeg-<timestamp>/recording.mp4
   frames    : /.../test/broadcast/out/ffmpeg-<timestamp>/frames.txt
   logs      : <tmp>/axonkit-broadcast-verify-ffmpeg.XXXXXX.../logs
==============================================================
```

Every run produces its own subdirectory under
`test/broadcast/out/<variant>-<timestamp>/` containing:

- `recording.mp4` — the recording (transcoded with `-c copy` so
  timestamps survive)
- `tNs-<phase>.png` — probe frames sampled at the midpoint of each
  phase (`t3s-live-A.png`, `t14s-mix-AB.png`, `t26s-live-A-again.png`
  for the default timings; the avflow variant also drops
  `tNs-mix-left.png` / `tNs-mix-right.png` for the composite halves)
- `frames.txt` — mean-RGB values per phase (one line each)

That directory is in `.gitignore`; feel free to
`rm -rf test/broadcast/out` whenever you want to tidy up.

## Phases and assertions

Both scripts run the same three-phase timeline:

| Phase | Name           | Expected recording                                              | Duration env var    |
| ----- | -------------- | --------------------------------------------------------------- | ------------------- |
| 1     | `live-A`       | solid red                                                       | `WARMUP_LIVE` (6 s) |
| 2     | `mix-AB`       | blue (ffmpeg variant) / composite of red+green (avflow variant) | `MIX_STABLE` (15 s) |
| 3     | `live-A-again` | solid red again                                                 | `POST_DETACH` (6 s) |

The assertions are mean-RGB checks on a probe frame sampled in the
middle of each phase, plus (for the avflow variant only) a halves
check that the composite is not a single colour. Failures leave the
MP4 + frame PNGs in `test/broadcast/out/<variant>-<timestamp>/` for
manual review.

## Environment variables

All variables are read by `lib.sh` at source time. Any script-specific
knobs are documented in that script's header comment.

### Timing (seconds)

| Variable            | Default | Meaning                      |
| ------------------- | ------- | ---------------------------- |
| `WARMUP_LIVE`       | 6       | live-only phase duration     |
| `MIX_STABLE`        | 15      | mix phase duration           |
| `POST_DETACH`       | 6       | post-detach phase duration   |
| `MIX_FLIP_TIMEOUT`  | 45      | max wait for live → mix flip |
| `LIVE_FLIP_TIMEOUT` | 15      | max wait for mix → live flip |

### Codec and ports

| Variable          | Default                   | Meaning                |
| ----------------- | ------------------------- | ---------------------- |
| `VIDEO_CODEC`     | `h264` (case-insensitive) | `h264` / `vp9` / `av1` (verify-broadcast end-to-end); `vp8` is accepted by the SFU path but the script's RTMP-only destinations will be rejected at `Broadcast.Start` (covered by unit test instead). |
| `HTTP_PORT`       | 17990                     | axonkit HTTP           |
| `WEBRTC_MIN_PORT` | 51200                     | WebRTC UDP range start |
| `WEBRTC_MAX_PORT` | 51400                     | WebRTC UDP range end   |
| `DST_RTMP_PORT`   | 11935                     | local RTMP recorder    |
| `ANNOUNCED_IP`    | 127.0.0.1                 | WebRTC announced IP    |

NATS runs in-process inside axonkit; there is no port to configure.
JetStream writes to `$STATE_DIR/nats/` for the duration of each run
and is removed when cleanup sweeps the temp dir.

### Paths

| Variable     | Default                | Meaning                                        |
| ------------ | ---------------------- | ---------------------------------------------- |
| `OUT_DIR`    | `test/broadcast/out`   | MP4 + PNG + frames.txt artefacts (git-ignored) |
| `WORKER_BIN` | `bin/mediasoup-worker` | mediasoup worker binary                        |

### avflow-specific (`verify-avflow.sh` only)

| Variable              | Default                   | Meaning                          |
| --------------------- | ------------------------- | -------------------------------- |
| `AVFLOW_REPO`         | `../avflow`               | path to the avflow checkout      |
| `AVFLOW_BIN`          | `$AVFLOW_REPO/bin/avflow` | avflow binary (rebuilt each run) |
| `AVFLOW_CONTROL_PORT` | 18090                     | avflow HTTP API                  |
| `MIX_CANVAS_W`        | 320                       | composite canvas width           |
| `MIX_CANVAS_H`        | 240                       | composite canvas height          |
| `MIX_CANVAS_FPS`      | 30                        | composite canvas FPS             |

`MIX_CANVAS_*` defaults match the live publishers' resolution/FPS so
avflow's WHIP egress negotiates the same codec configuration as the
live users. That way the axonkit RTMP pusher observes byte-identical
SPS/PPS (or `vpcC` / `AV1CC` for E-RTMP) across live↔mix flips and
suppresses the mid-stream SeqHeader tag that would otherwise cause
ffmpeg's RTMP demuxer to drop its TCP socket. Override these if you
want to test the codec-change path explicitly.

## Artefacts and debugging

### On success

All keeper artefacts go to `test/broadcast/out/<variant>-<timestamp>/`:

- `recording.mp4` — the recording (transcoded with `-c copy` so
  timestamps survive)
- `tNs-<phase>.png` — probe frames at the phase midpoints
  (`t3s-live-A.png`, `t14s-mix-AB.png`, `t26s-live-A-again.png` for
  default timings, plus `tNs-mix-{left,right}.png` on the avflow
  variant)
- `frames.txt` — mean RGB values per phase

### On failure

The run preserves its `STATE_DIR` (the final lines of the cleanup
output print its full path). That directory contains:

- `logs/axonkit.log` — axonkit + its embedded NATS server (look for
  `NATS URL is empty, using in-process NATS server` near the top)
- `logs/publisher-user{A,B}.log`, `logs/publisher-user{A,B}.log.ffmpeg`
  — WHIP publishers (ffmpeg + whipsim when applicable)
- `logs/mix-whip.log`, `logs/avflow.log` — mix source
- `logs/recorder.log` — the RTMP listener (one entry per segment;
  exactly one entry is expected when codec configs match)
- `nats/` — JetStream data directory of the embedded NATS server
- `rec/seg-*.flv` — raw FLV segments captured off RTMP
- `rec/out.flv` — concatenated capture

A common failure path: `[recorder] captured N segment(s)` with N > 1
means the RTMP receiver saw a mid-stream SeqHeader and reconnected.
Check `axonkit.log` for the switcher flip events (`onFlipTo{Live,Mix}`)
and verify whether the live and mix sources actually share the same
encoder parameters.

## Architecture notes

Relevant internals in `pkg/broadcast/`:

- `rtmpPusher.Ensure{Ex,}VideoSeqHeader` caches the last-emitted codec
  config record and skips redundant SeqHeader tags on `ResetVideoHeader`
  — the mechanism that keeps the RTMP TCP socket alive across flips
  when live and mix negotiate identical codec params.
- `liveSource` / `mixSource` each own a `videoDepacketizer` interface
  implementation (`H264Depacketizer`, `VP9Depacketizer`,
  `AV1Depacketizer`) that extracts both access units and the codec
  configuration record needed by the pusher.
- `cmd/whipsim` is the fallback WHIP publisher for VP9 / AV1 streams
  because `ffmpeg -f whip` is H.264-only. It consumes an IVF video
  pipe + Ogg/Opus audio pipe written live by ffmpeg.

See the source file headers for the full story.
