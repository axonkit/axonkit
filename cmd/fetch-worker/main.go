// fetch-worker downloads the prebuilt mediasoup-worker binary that matches
// the mediasoup-go version recorded in go.mod. `make run` and `make test`
// invoke it so a fresh checkout is runnable without manual setup:
//
//	go run ./cmd/fetch-worker [-o path]
//
// The worker version is read from mediasoup.MEDIASOUP_WORKER_VERSION (the
// same constant mediasoup-go enforces when spawning the worker), so a go.mod
// upgrade automatically invalidates the cached download via a marker file
// written next to the binary. Release assets live at
// https://github.com/versatica/mediasoup/releases and are named after the
// target platform, with Linux builds split by kernel major version
// (kernel6/kernel7) — see getWorkerPrebuildTarName() in mediasoup's
// npm-scripts.mjs.
package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

const releaseURL = "https://github.com/versatica/mediasoup/releases/download/%s/%s"

// markerFile records the worker version the binary was fetched for.
const markerFile = ".mediasoup-version"

func main() {
	out := flag.String("o", defaultOutput(), "output path for the worker binary")
	flag.Parse()

	version := mediasoup.MEDIASOUP_WORKER_VERSION
	if upToDate(*out, version) {
		fmt.Printf("mediasoup-worker %s already present: %s\n", version, *out)
		return
	}

	flavor, err := assetFlavor()
	if err != nil {
		fatal(err)
	}
	asset := fmt.Sprintf("mediasoup-worker-%s-%s.tgz", version, flavor)
	url := fmt.Sprintf(releaseURL, version, asset)

	if err := fetchAndExtract(url, asset, *out); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(markerPath(*out), []byte(version), 0o644); err != nil {
		fatal(fmt.Errorf("write version marker: %w", err))
	}
	fmt.Printf("downloaded mediasoup-worker %s (%s) -> %s\n", version, flavor, *out)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "fetch-worker: %v\n", err)
	os.Exit(1)
}

// defaultOutput mirrors the server's -worker flag default so `make run` works
// with no extra configuration.
func defaultOutput() string {
	name := "mediasoup-worker"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return name
}

// markerPath sits next to the binary so moving it invalidates the cache.
func markerPath(out string) string {
	return filepath.Join(filepath.Dir(out), markerFile)
}

func upToDate(out, version string) bool {
	if _, err := os.Stat(out); err != nil {
		return false
	}
	marker, err := os.ReadFile(markerPath(out))
	return err == nil && string(marker) == version
}

// assetFlavor maps the host platform to the mediasoup release asset flavor.
// Linux tarballs are built against the oldest supported glibc per kernel
// major version (see versatica/mediasoup#1089), so we match the host kernel.
func assetFlavor() (string, error) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "darwin/arm64":
		return "darwin-arm64", nil
	case "windows/amd64":
		return "win32-x64", nil
	case "linux/amd64", "linux/arm64":
		flavor := "linux-x64"
		if runtime.GOARCH == "arm64" {
			flavor = "linux-arm64"
		}
		kernel, err := linuxKernelMajor()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s-kernel%d", flavor, kernel), nil
	}
	return "", fmt.Errorf("no prebuilt mediasoup-worker for %s/%s; build the worker from source instead", runtime.GOOS, runtime.GOARCH)
}

// linuxKernelMajor returns the host kernel major version (6 or 7).
func linuxKernelMajor() (int, error) {
	out, err := exec.Command("uname", "-r").Output()
	if err != nil {
		return 0, fmt.Errorf("detect kernel version: %w", err)
	}
	major, err := strconv.Atoi(strings.SplitN(strings.TrimSpace(string(out)), ".", 2)[0])
	if err != nil {
		return 0, fmt.Errorf("parse kernel version: %w", err)
	}
	if major >= 7 {
		return 7, nil
	}
	if major >= 6 {
		return 6, nil
	}
	return 0, fmt.Errorf("kernel %d.x has no prebuilt mediasoup-worker (requires 6.x or newer)", major)
}

// fetchAndExtract downloads the tarball and writes the worker binary to out
// atomically (temp file + rename).
func fetchAndExtract(url, asset, out string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("download %s: %w", asset, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: unexpected status %d", asset, resp.StatusCode)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("decompress %s: %w", asset, err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%s does not contain a mediasoup-worker binary", asset)
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", asset, err)
		}
		base := filepath.Base(hdr.Name)
		if hdr.Typeflag != tar.TypeReg || (base != "mediasoup-worker" && base != "mediasoup-worker.exe") {
			continue
		}
		return writeWorker(tr, out)
	}
}

func writeWorker(r io.Reader, out string) error {
	if dir := filepath.Dir(out); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create output dir: %w", err)
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(out), ".fetch-worker-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", out, err)
	}
	if runtime.GOOS != "windows" {
		if err := tmp.Chmod(0o755); err != nil {
			tmp.Close()
			return fmt.Errorf("chmod %s: %w", out, err)
		}
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", out, err)
	}
	return os.Rename(tmp.Name(), out)
}
