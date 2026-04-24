package broadcast

import "testing"

func TestParseRTMPURL(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantHost  string
		wantApp   string
		wantKey   string
		wantError bool
	}{
		{
			name:     "basic",
			in:       "rtmp://example.com/live/stream1",
			wantHost: "example.com:1935",
			wantApp:  "live",
			wantKey:  "stream1",
		},
		{
			name:     "explicit port",
			in:       "rtmp://example.com:1936/live/stream1",
			wantHost: "example.com:1936",
			wantApp:  "live",
			wantKey:  "stream1",
		},
		{
			name:     "query string in key",
			in:       "rtmp://example.com/live/stream1?token=abc",
			wantHost: "example.com:1935",
			wantApp:  "live",
			wantKey:  "stream1?token=abc",
		},
		{
			name:      "missing stream key",
			in:        "rtmp://example.com/live",
			wantError: true,
		},
		{
			name:      "bad scheme",
			in:        "http://example.com/live/stream1",
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRTMPURL(tt.in)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", got.Host, tt.wantHost)
			}
			if got.App != tt.wantApp {
				t.Errorf("app = %q, want %q", got.App, tt.wantApp)
			}
			if got.StreamKey != tt.wantKey {
				t.Errorf("streamKey = %q, want %q", got.StreamKey, tt.wantKey)
			}
		})
	}
}
