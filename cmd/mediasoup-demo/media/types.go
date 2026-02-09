package media

import (
	"rib-control/pkg/types"

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
	PipeInfo       PipeInfo                `json:"pipeInfo,omitempty"`
	OriginRouterId string                  `json:"originRouterId,omitempty"`
	PipeProducerId string                  `json:"pipeProducerId,omitempty"` // For Consume
	RtpParameters  mediasoup.RtpParameters `json:"rtpParameters,omitempty"`
	Error          string                  `json:"error,omitempty"`
}
