package broadcast

import (
	"fmt"
	"net/url"
	"strings"
)

// rtmpURL is the parsed form of an rtmp:// or rtmps:// push target.
type rtmpURL struct {
	Scheme    string
	Host      string
	App       string
	StreamKey string
}

// parseRTMPURL splits rtmp://host[:port]/app/streamkey into its four
// pieces. If the port is absent it defaults to 1935 (classic RTMP).
// Query string, if present, is appended to StreamKey so token-based
// ingest points that expect "streamkey?token=..." still round-trip.
func parseRTMPURL(rawURL string) (*rtmpURL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	if u.Scheme != "rtmp" && u.Scheme != "rtmps" {
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	host := u.Host
	if host == "" {
		return nil, fmt.Errorf("rtmp url missing host")
	}
	if !strings.Contains(host, ":") {
		host += ":1935"
	}

	path := strings.TrimPrefix(u.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid rtmp path, expected /app/stream-key: %s", u.Path)
	}
	streamKey := parts[1]
	if u.RawQuery != "" {
		streamKey = streamKey + "?" + u.RawQuery
	}

	return &rtmpURL{
		Scheme:    u.Scheme,
		Host:      host,
		App:       parts[0],
		StreamKey: streamKey,
	}, nil
}
