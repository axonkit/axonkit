package rtmp

import "testing"

// These tests cover the pure helpers that translate codec-config
// records into the RtpParameters handed to mediasoup. They do not
// exercise the DirectTransport / Producer creation path (which needs a
// live worker) — instead they guard the negotiation knobs that must
// match the entries declared in pkg/media/caps.go, otherwise the
// mediasoup Router would reject Producer creation.

func TestBuildH264RtpParameters(t *testing.T) {
	// Build a minimal valid AVCDecoderConfigurationRecord so
	// ProfileLevelID() has real bytes to read.
	sps := []byte{0x67, 0x42, 0x00, 0x1F, 0xAA, 0xBB}
	pps := []byte{0x68, 0xCE, 0x38, 0x80}
	rec := &AVCDecoderConfigurationRecord{
		ProfileIdc:    0x42,
		ProfileCompat: 0x00,
		LevelIdc:      0x1F,
		SPS:           [][]byte{sps},
		PPS:           [][]byte{pps},
	}
	params := buildH264RtpParameters(0xABCDEF01, videoH264PT, rec)
	if len(params.Codecs) != 1 {
		t.Fatalf("expected 1 codec entry, got %d", len(params.Codecs))
	}
	c := params.Codecs[0]
	if c.MimeType != "video/H264" || c.PayloadType != videoH264PT || c.ClockRate != 90000 {
		t.Fatalf("bad codec params: %+v", c)
	}
	if c.Parameters.PacketizationMode != 1 || c.Parameters.LevelAsymmetryAllowed != 1 {
		t.Fatalf("bad fmtp params: %+v", c.Parameters)
	}
	if c.Parameters.ProfileLevelId != "42001f" {
		t.Fatalf("ProfileLevelId = %q, want 42001f", c.Parameters.ProfileLevelId)
	}
	if params.Encodings[0].Ssrc != 0xABCDEF01 {
		t.Fatalf("ssrc mismatch: %x", params.Encodings[0].Ssrc)
	}
}

func TestBuildVP9RtpParameters_Profile0(t *testing.T) {
	rec := &VPCodecConfigurationRecord{Profile: 0}
	params := buildVP9RtpParameters(1, videoVP9PT, rec)
	if params.Codecs[0].MimeType != "video/VP9" {
		t.Fatalf("mime = %q", params.Codecs[0].MimeType)
	}
	if params.Codecs[0].Parameters.ProfileId != 0 {
		t.Fatalf("profile-id = %d, want 0", params.Codecs[0].Parameters.ProfileId)
	}
}

func TestBuildVP9RtpParameters_ClampsUnsupportedProfile(t *testing.T) {
	// Profile 1 and 3 are outside what caps.go advertises; the builder
	// must clamp to 0 so Producer creation does not fail.
	rec := &VPCodecConfigurationRecord{Profile: 1}
	params := buildVP9RtpParameters(1, videoVP9PT, rec)
	if params.Codecs[0].Parameters.ProfileId != 0 {
		t.Fatalf("profile-id = %d, want clamped 0", params.Codecs[0].Parameters.ProfileId)
	}
}

func TestBuildVP9RtpParameters_KeepsProfile2(t *testing.T) {
	rec := &VPCodecConfigurationRecord{Profile: 2}
	params := buildVP9RtpParameters(1, videoVP9PT, rec)
	if params.Codecs[0].Parameters.ProfileId != 2 {
		t.Fatalf("profile-id = %d, want 2", params.Codecs[0].Parameters.ProfileId)
	}
}

func TestBuildAV1RtpParameters(t *testing.T) {
	rec := &AV1CodecConfigurationRecord{SeqProfile: 1, SeqLevelIdx0: 8}
	params := buildAV1RtpParameters(0xDEAD, videoAV1PT, rec)
	if params.Codecs[0].MimeType != "video/AV1" {
		t.Fatalf("mime = %q", params.Codecs[0].MimeType)
	}
	if params.Codecs[0].ClockRate != 90000 {
		t.Fatalf("clock rate = %d", params.Codecs[0].ClockRate)
	}
	if params.Codecs[0].Parameters.ProfileId != 1 {
		t.Fatalf("profile-id = %d, want 1", params.Codecs[0].Parameters.ProfileId)
	}
	// Every video leg inherits the standard NACK/PLI/FIR feedback set.
	if len(params.Codecs[0].RtcpFeedback) < 3 {
		t.Fatalf("missing rtcp feedback entries: %+v", params.Codecs[0].RtcpFeedback)
	}
}
