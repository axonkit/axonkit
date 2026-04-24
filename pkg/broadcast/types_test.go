package broadcast

import (
	"errors"
	"testing"
)

func TestValidateDestinations(t *testing.T) {
	tests := []struct {
		name    string
		dsts    []Destination
		wantErr error
	}{
		{
			name:    "empty",
			dsts:    nil,
			wantErr: ErrNoDestinations,
		},
		{
			name: "too many",
			dsts: []Destination{
				{Type: DestinationRTMP, URL: "rtmp://a/app/1"},
				{Type: DestinationRTMP, URL: "rtmp://b/app/2"},
				{Type: DestinationRTMP, URL: "rtmp://c/app/3"},
				{Type: DestinationRTMP, URL: "rtmp://d/app/4"},
			},
			wantErr: ErrTooManyDestinations,
		},
		{
			name: "duplicate url",
			dsts: []Destination{
				{Type: DestinationRTMP, URL: "rtmp://a/app/1"},
				{Type: DestinationRTMP, URL: "rtmp://a/app/1"},
			},
		},
		{
			name: "unknown type",
			dsts: []Destination{{Type: "srt", URL: "srt://a/b"}},
		},
		{
			name: "one rtmp + one whip",
			dsts: []Destination{
				{Type: DestinationRTMP, URL: "rtmp://a/app/1"},
				{Type: DestinationWHIP, URL: "https://whip/x"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.dsts)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("got %v want %v", err, tt.wantErr)
				}
				return
			}
			switch tt.name {
			case "one rtmp + one whip":
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			default:
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
			}
		})
	}
}
