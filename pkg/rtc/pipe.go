package rtc

import (
	"context"
	"fmt"
	"sync"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"golang.org/x/sync/singleflight"
)

// RouterPipe memoizes PipeToRouter operations that originate from a single
// source router. It lets multiple callers that need the same source
// producer to land on a given destination router share one underlying
// pipe, and coalesces concurrent cache misses via singleflight.
//
// Cache entries are invalidated when the piped producer closes — which
// happens if either the source producer or the destination router is torn
// down — so stale IDs cannot linger.
//
// The helper is used from two places today:
//
//   - rtc.Participant, when one subscriber's receive router differs from
//     the publishing participant's router (cross-router SFU subscribe);
//   - server WHEP handler, when the consumer transport is allocated on a
//     router other than the publisher's (cross-router WHEP subscribe).
//
// Both paths share the same `(srcProducerId, destRouterId) -> pipedId`
// table so a single pipe is reused across signaling protocols.
type RouterPipe struct {
	source *mediasoup.Router
	sf     singleflight.Group
	mu     sync.Mutex
	cache  map[string]string
}

// NewRouterPipe constructs an empty RouterPipe rooted at source.
func NewRouterPipe(source *mediasoup.Router) *RouterPipe {
	return &RouterPipe{source: source, cache: make(map[string]string)}
}

// Source returns the router that owns the producers piped through this
// helper.
func (r *RouterPipe) Source() *mediasoup.Router { return r.source }

// pipeEndpoint is the slice of Producer and DataProducer the cache needs:
// the piped id plus a close hook to evict the entry.
type pipeEndpoint interface {
	Id() string
	OnClose(listener func(context.Context)) func()
}

// ProducerTo pipes srcProducerId from the source router into dest and
// returns the piped producer ID on dest. If source and dest are the same
// router the original id is returned with no side effects.
func (r *RouterPipe) ProducerTo(srcProducerId string, dest *mediasoup.Router) (string, error) {
	return r.run("p:", srcProducerId, dest, func() (*mediasoup.PipeToRouterResult, error) {
		return r.source.PipeToRouter(&mediasoup.PipeToRouterOptions{
			ProducerId: srcProducerId,
			Router:     dest,
			ListenInfo: loopbackListenInfo,
		})
	}, func(res *mediasoup.PipeToRouterResult) (pipeEndpoint, error) {
		if res.PipeProducer == nil {
			return nil, fmt.Errorf("PipeToRouter returned no PipeProducer")
		}
		return res.PipeProducer, nil
	})
}

// DataProducerTo is the data-channel counterpart of ProducerTo.
func (r *RouterPipe) DataProducerTo(srcDataProducerId string, dest *mediasoup.Router) (string, error) {
	return r.run("d:", srcDataProducerId, dest, func() (*mediasoup.PipeToRouterResult, error) {
		return r.source.PipeToRouter(&mediasoup.PipeToRouterOptions{
			DataProducerId: srcDataProducerId,
			Router:         dest,
			ListenInfo:     loopbackListenInfo,
		})
	}, func(res *mediasoup.PipeToRouterResult) (pipeEndpoint, error) {
		if res.PipeDataProducer == nil {
			return nil, fmt.Errorf("PipeToRouter returned no PipeDataProducer")
		}
		return res.PipeDataProducer, nil
	})
}

// loopbackListenInfo mirrors the config used by the original inline pipes:
// UDP on localhost because pipe producers only traverse worker boundaries
// within the same node, and loopback UDP is the cheapest transport.
var loopbackListenInfo = mediasoup.TransportListenInfo{
	Ip:       "127.0.0.1",
	Protocol: mediasoup.TransportProtocolUDP,
}

// run is the shared body of ProducerTo / DataProducerTo: it short-circuits
// on same-router destinations, serves from cache, otherwise coalesces
// concurrent pipe builds and installs an OnClose hook to evict the entry.
// The two kind-specific callbacks handle the PipeToRouterOptions build and
// the result unpacking (piped endpoint).
func (r *RouterPipe) run(
	prefix, srcId string,
	dest *mediasoup.Router,
	build func() (*mediasoup.PipeToRouterResult, error),
	unpack func(*mediasoup.PipeToRouterResult) (pipeEndpoint, error),
) (string, error) {
	if r == nil || r.source == nil {
		return "", fmt.Errorf("router pipe has no source")
	}
	if dest == nil {
		return "", fmt.Errorf("destination router is nil")
	}
	if r.source.Id() == dest.Id() {
		return srcId, nil
	}
	key := prefix + srcId + ":" + dest.Id()
	if id, ok := r.get(key); ok {
		return id, nil
	}
	v, err, _ := r.sf.Do(key, func() (any, error) {
		if id, ok := r.get(key); ok {
			return id, nil
		}
		res, err := build()
		if err != nil {
			return "", err
		}
		ep, err := unpack(res)
		if err != nil {
			return "", err
		}
		id := ep.Id()
		r.put(key, id)
		ep.OnClose(func(context.Context) {
			r.drop(key, id)
		})
		return id, nil
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (r *RouterPipe) get(key string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.cache[key]
	return id, ok
}

func (r *RouterPipe) put(key, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[key] = id
}

func (r *RouterPipe) drop(key, expected string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.cache[key]; ok && cur == expected {
		delete(r.cache, key)
	}
}
