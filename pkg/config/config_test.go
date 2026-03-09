package config

import "testing"

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.Node.Port != 7990 {
		t.Fatalf("expected port 7990, got %d", cfg.Node.Port)
	}
	if !cfg.Room.Dynacast {
		t.Fatal("expected Dynacast enabled by default")
	}
	if !cfg.Room.AutoSubscribe {
		t.Fatal("expected AutoSubscribe enabled by default")
	}
	if cfg.Room.MaxParticipants != 100 {
		t.Fatalf("expected 100 max participants, got %d", cfg.Room.MaxParticipants)
	}
}

func TestConfig_PingInterval(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PingInterval() != 5 {
		t.Fatalf("expected ping interval 5, got %d", cfg.PingInterval())
	}
}

func TestConfig_PingTimeout(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PingTimeout() != 15 {
		t.Fatalf("expected ping timeout 15, got %d", cfg.PingTimeout())
	}
}
