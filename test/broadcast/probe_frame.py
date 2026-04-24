#!/usr/bin/env python3
"""Probe a PNG frame and print its mean RGB.

Usage:
    probe_frame.py <png>

Output format:
    R,G,B

Exit code is 0 on success and non-zero on decode failure.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def decode_rgb(png: Path) -> bytes:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-i", str(png),
        "-vf", "scale=32:32,format=rgb24,crop=24:24:4:4",
        "-f", "rawvideo",
        "-",
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True)
    return proc.stdout


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: probe_frame.py <png>", file=sys.stderr)
        return 2
    png = Path(sys.argv[1])
    if not png.is_file():
        print(f"missing file: {png}", file=sys.stderr)
        return 2

    raw = decode_rgb(png)
    if not raw:
        print("empty RGB buffer", file=sys.stderr)
        return 2

    pixels = len(raw) // 3
    r = sum(raw[i * 3 + 0] for i in range(pixels)) / pixels
    g = sum(raw[i * 3 + 1] for i in range(pixels)) / pixels
    b = sum(raw[i * 3 + 2] for i in range(pixels)) / pixels
    print(f"{int(r)},{int(g)},{int(b)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
