package broadcast

import (
	"strings"
	"testing"
)

// TestValidateDestinationsForCodec pins the VP8 + RTMP rejection rule
// covered by the patent's claim 7. Other codecs are explicitly allowed
// against any destination mix; the only hard "no" is VP8 + RTMP, since
// Enhanced-RTMP v2 has not registered a FourCC for VP8 and the pushers
// have no way to wire a sample-to-tag function.
func TestValidateDestinationsForCodec(t *testing.T) {
	rtmp := Destination{Type: DestinationRTMP, URL: "rtmp://x/app/1"}
	whip := Destination{Type: DestinationWHIP, URL: "https://x/whip"}

	cases := []struct {
		name    string
		codec   Codec
		dsts    []Destination
		wantErr bool
	}{
		{name: "h264_rtmp_ok", codec: CodecH264, dsts: []Destination{rtmp}},
		{name: "h264_whip_ok", codec: CodecH264, dsts: []Destination{whip}},
		{name: "vp9_rtmp_ok", codec: CodecVP9, dsts: []Destination{rtmp}},
		{name: "av1_mixed_ok", codec: CodecAV1, dsts: []Destination{rtmp, whip}},
		{name: "h265_rtmp_ok", codec: CodecH265, dsts: []Destination{rtmp}},
		{name: "vp8_whip_only_ok", codec: CodecVP8, dsts: []Destination{whip}},
		{name: "vp8_two_whip_ok", codec: CodecVP8, dsts: []Destination{whip, whip}},

		{name: "vp8_rtmp_rejected", codec: CodecVP8, dsts: []Destination{rtmp}, wantErr: true},
		{name: "vp8_mixed_rejected", codec: CodecVP8, dsts: []Destination{whip, rtmp}, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDestinationsForCodec(tc.dsts, tc.codec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "VP8") {
					t.Fatalf("error message should mention VP8, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
