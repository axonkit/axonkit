package media

import (
	"github.com/jiyeyuran/omnistream/pkg/types"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

type PipeInfo struct {
	Ip             string                    `json:"ip"`
	Port           uint16                    `json:"port"`
	SrtpParameters *mediasoup.SrtpParameters `json:"srtpParameters"`
}

type PipeRequest struct {
	Command           string         `json:"command"` // "EstablishPipe"
	StreamID          types.StreamID `json:"streamId,omitempty"`
	RequesterNode     types.NodeID   `json:"requesterNode,omitempty"`
	RequesterRouterId string         `json:"requesterRouterId,omitempty"`
	PipeInfo          PipeInfo       `json:"pipeInfo,omitempty"`
	PipeConsumerId    string         `json:"pipeConsumerId,omitempty"` // For ClosePipe
}

type PipeResponse struct {
	PipeInfo       PipeInfo     `json:"pipeInfo,omitempty"`
	OriginRouterId string       `json:"originRouterId,omitempty"`
	PipeConsumerId string       `json:"pipeConsumerId,omitempty"`
	ProducerInfo   ProducerInfo `json:"producerInfo,omitempty"`
	Error          string       `json:"error,omitempty"`
}

type ProducerInfo struct {
	Id            string                   `json:"id,omitempty"`
	Kind          mediasoup.MediaKind      `json:"kind,omitempty"`
	RtpParameters *mediasoup.RtpParameters `json:"rtpParameters,omitempty"`
	Paused        bool                     `json:"paused,omitempty"`
	AppData       map[string]any           `json:"appData,omitempty"`
}
