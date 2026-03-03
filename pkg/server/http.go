package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jiyeyuran/omnistream/pkg/discovery"
	"github.com/jiyeyuran/omnistream/pkg/media"
	proto "github.com/jiyeyuran/omnistream/pkg/proto"
	"github.com/jiyeyuran/omnistream/pkg/routing"
	"github.com/jiyeyuran/omnistream/pkg/types"
)

type connectionIDKey struct{}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	return n, err
}

func WithLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectionID := r.Header.Get("X-CONNECTION-ID")
		if connectionID == "" {
			writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{Error: "missing X-CONNECTION-ID"})
			return
		}
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w}
		ctx := context.WithValue(r.Context(), connectionIDKey{}, connectionID)
		next.ServeHTTP(lrw, r.WithContext(ctx))
		slog.Info("HTTP request", "method", r.Method, "path", r.URL.Path, "status", lrw.status, "bytes", lrw.bytes, "connection_id", connectionID, "duration_ms", time.Since(start).Milliseconds())
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func RegisterHTTPRoutes(mux *http.ServeMux, mediaMgr *media.Manager, rib *routing.RIB, fib *routing.FIB, finder *discovery.Finder, node types.NodeInfo) {
	mux.HandleFunc("GET /publish", func(w http.ResponseWriter, r *http.Request) {
		streamID, err := mediaMgr.Publish()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		err = rib.StartAdvertise(routing.AdvertiseOptions{
			StreamID:    streamID,
			BaseCost:    0,
			PathVector:  []types.NodeID{node.ID},
			RttToOrigin: 0,
			IsOrigin:    true,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Published & Advertised %s\n", streamID)
	})

	mux.HandleFunc("GET /subscribe", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		clientID := r.URL.Query().Get("client")
		if streamID == "" || clientID == "" {
			http.Error(w, "missing stream or client", http.StatusBadRequest)
			return
		}
		pipeID, upstream, err := fib.Subscribe(types.StreamID(streamID), finder.FindPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Subscribe failed: %v", err), http.StatusInternalServerError)
			return
		}
		consumer, err := mediaMgr.Consume(clientID, types.StreamID(streamID))
		if err != nil {
			http.Error(w, fmt.Sprintf("Consume failed: %v", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Subscribed %s via %s (Consumer: %s, Pipe: %s)\n", streamID, upstream, consumer.Id(), pipeID)
	})

	mux.HandleFunc("GET /unpublish", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		if streamID == "" {
			http.Error(w, "missing stream", http.StatusBadRequest)
			return
		}
		_ = mediaMgr.Unpublish(types.StreamID(streamID))
		_ = rib.StopAdvertise(types.StreamID(streamID))
		fmt.Fprintf(w, "Unpublished %s\n", streamID)
	})

	mux.HandleFunc("GET /unsubscribe", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		clientID := r.URL.Query().Get("client")
		if streamID == "" || clientID == "" {
			http.Error(w, "missing stream or client", http.StatusBadRequest)
			return
		}
		_ = mediaMgr.Unsubscribe(clientID, types.StreamID(streamID))
		fib.Unsubscribe(types.StreamID(streamID))
		fmt.Fprintf(w, "Unsubscribed %s\n", streamID)
	})
}
