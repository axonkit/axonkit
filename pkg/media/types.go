package media

import (
	"github.com/jiyeyuran/omnistream/pkg/types"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

type PipeInfo struct {
	Ip             string `json:"ip"`
	Port           uint16 `json:"port"`
	SctpParameters *mediasoup.SctpParameters
	SrtpParameters *mediasoup.SrtpParameters `json:"srtpParameters"`
}

func NewPipeInfoFromTransportData(transportData *mediasoup.PipeTransportData) PipeInfo {
	return PipeInfo{
		Ip:             transportData.Tuple.LocalAddress,
		Port:           transportData.Tuple.LocalPort,
		SctpParameters: transportData.SctpParameters,
		SrtpParameters: transportData.SrtpParameters,
	}
}

type PipeRequest struct {
	Command           string                         `json:"command"`
	StreamID          types.StreamID                 `json:"streamId,omitempty"`
	RequesterNode     types.NodeID                   `json:"requesterNode,omitempty"`
	RequesterRouterId string                         `json:"requesterRouterId,omitempty"`
	PipeInfo          PipeInfo                       `json:"pipeInfo,omitempty"`
	PipeConsumerId    string                         `json:"pipeConsumerId,omitempty"` // For ClosePipe
	DataProducerInfo  *mediasoup.DataProducerOptions `json:"dataProducerInfo,omitempty"`
}

type PipeResponse struct {
	PipeInfo         PipeInfo                       `json:"pipeInfo,omitempty"`
	PipeConsumerId   string                         `json:"pipeConsumerId,omitempty"`
	ProducerInfo     *mediasoup.ProducerOptions     `json:"producerInfo,omitempty"`
	DataProducerInfo *mediasoup.DataProducerOptions `json:"dataProducerInfo,omitempty"`
	Error            string                         `json:"error,omitempty"`
}
