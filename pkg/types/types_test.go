package types

import (
	"testing"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

func TestNewStreamID(t *testing.T) {
	sid := NewStreamID("router-123", "producer-456")
	if sid != "router-123/producer-456" {
		t.Fatalf("expected router-123/producer-456, got %s", sid)
	}
}

func TestStreamID_RouterID(t *testing.T) {
	tests := []struct {
		input    StreamID
		expected string
	}{
		{"router-abc/producer-def", "router-abc"},
		{"single", "single"},
	}
	for _, tc := range tests {
		got := tc.input.RouterID()
		if got != tc.expected {
			t.Errorf("StreamID(%q).RouterID() = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestStreamID_ProducerID(t *testing.T) {
	tests := []struct {
		input    StreamID
		expected string
	}{
		{"router-abc/producer-def", "producer-def"},
		{"single", "single"},
	}
	for _, tc := range tests {
		got := tc.input.ProducerID()
		if got != tc.expected {
			t.Errorf("StreamID(%q).ProducerID() = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestStreamID_IsDataProducer(t *testing.T) {
	data := NewStreamID("rt-abc", mediasoup.DataProducerIDPrefix+"uuid")
	if !data.IsDataProducer() {
		t.Fatal("expected IsDataProducer = true for dp- prefix")
	}

	media := NewStreamID("rt-abc", "pr-uuid")
	if media.IsDataProducer() {
		t.Fatal("expected IsDataProducer = false for pr- prefix")
	}
}
