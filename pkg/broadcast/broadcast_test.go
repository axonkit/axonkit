package broadcast

import (
	"io"
	"log/slog"
	"testing"
)

func TestHandleMixGoneClearsMixStatus(t *testing.T) {
	b := &Broadcast{
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		mixConnected: true,
		mixIceState:  "connected",
	}

	b.handleMixGone("ice failed")
	st := b.Status()

	if st.MixConnected {
		t.Fatal("MixConnected = true, want false")
	}
	if st.MixIceState != "" {
		t.Fatalf("MixIceState = %q, want empty", st.MixIceState)
	}
}
