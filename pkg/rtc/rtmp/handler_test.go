package rtmp

import "testing"

func TestParseStreamKey(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantRoom   string
		wantToken  string
		wantHasTok bool
	}{
		{
			name:       "bare room (anonymous publish)",
			in:         "demo",
			wantRoom:   "demo",
			wantToken:  "",
			wantHasTok: false,
		},
		{
			name:       "room with token query",
			in:         "demo?token=abc.def.ghi",
			wantRoom:   "demo",
			wantToken:  "abc.def.ghi",
			wantHasTok: true,
		},
		{
			name:       "extra query params are ignored",
			in:         "live42?token=jwt&client=ffmpeg",
			wantRoom:   "live42",
			wantToken:  "jwt",
			wantHasTok: true,
		},
		{
			name:       "empty token query still treated as anonymous",
			in:         "demo?token=",
			wantRoom:   "demo",
			wantToken:  "",
			wantHasTok: false,
		},
		{
			name:       "whitespace is trimmed",
			in:         "  demo  ",
			wantRoom:   "demo",
			wantToken:  "",
			wantHasTok: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			room, tok, has := ParseStreamKey(c.in)
			if room != c.wantRoom {
				t.Fatalf("room: want %q got %q", c.wantRoom, room)
			}
			if tok != c.wantToken {
				t.Fatalf("token: want %q got %q", c.wantToken, tok)
			}
			if has != c.wantHasTok {
				t.Fatalf("hasToken: want %v got %v", c.wantHasTok, has)
			}
		})
	}
}
