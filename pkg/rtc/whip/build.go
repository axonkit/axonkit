package whip

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/pion/sdp/v3"
)

// MediaAnswer is the per-m-section material fed into BuildAnswer.
// For WHIP the server mirrors the offer's PTs and codecs back via
// RtpParameters (populated by ParseOffer). For WHEP the server picks
// its own ssrcs/encodings from mediasoup.Consumer and mirrors the
// offer PTs it accepts.
type MediaAnswer struct {
	Mid        string
	Kind       mediasoup.MediaKind
	Direction  mediasoup.MediaDirection
	RtpParams  *mediasoup.RtpParameters
	OfferPTs   []uint8 // PTs that were present in the offer (same order)
	OfferProto []string
	Msid       string
	// WHEP-only: the ssrcs that the server will use when sending RTP.
	ServerSsrc    uint32
	ServerRtxSsrc uint32
	ServerCname   string
}

// AnswerParams is the full set of inputs required to build a WHIP/WHEP
// answer SDP. IceParameters/IceCandidates/DtlsParameters come directly
// from a mediasoup WebRtcTransport.
type AnswerParams struct {
	IceParameters  *mediasoup.IceParameters
	IceCandidates  []mediasoup.IceCandidate
	DtlsParameters *mediasoup.DtlsParameters
	DtlsSetup      string // "active" or "passive" (usually "active" for SFU)
	Medias         []*MediaAnswer
}

// BuildAnswer synthesizes an RFC 9725 / RFC 3264 compliant answer SDP given
// transport parameters and per-m-section rtp parameters. The answer uses
// BUNDLE, rtcp-mux and a=setup:active by default (passive for WHEP consumer
// direction in practice works but active is safe because ICE role is fixed).
func BuildAnswer(p *AnswerParams) (string, error) {
	if p == nil || p.IceParameters == nil || p.DtlsParameters == nil {
		return "", fmt.Errorf("missing transport parameters")
	}
	if len(p.Medias) == 0 {
		return "", fmt.Errorf("no medias in answer")
	}

	var fingerprint mediasoup.DtlsFingerprint
	if len(p.DtlsParameters.Fingerprints) > 0 {
		fingerprint = p.DtlsParameters.Fingerprints[0]
		for _, f := range p.DtlsParameters.Fingerprints {
			if strings.EqualFold(f.Algorithm, "sha-256") {
				fingerprint = f
				break
			}
		}
	}
	if fingerprint.Value == "" {
		return "", fmt.Errorf("no DTLS fingerprint available")
	}

	setup := p.DtlsSetup
	if setup == "" {
		setup = "active"
	}

	mids := make([]string, 0, len(p.Medias))
	for _, m := range p.Medias {
		mids = append(mids, m.Mid)
	}

	sess := &sdp.SessionDescription{
		Version: 0,
		Origin: sdp.Origin{
			Username:       "-",
			SessionID:      uint64(time.Now().UnixNano()),
			SessionVersion: 2,
			NetworkType:    "IN",
			AddressType:    "IP4",
			UnicastAddress: "0.0.0.0",
		},
		SessionName: "-",
		TimeDescriptions: []sdp.TimeDescription{{Timing: sdp.Timing{}}},
		Attributes: []sdp.Attribute{
			{Key: "group", Value: "BUNDLE " + strings.Join(mids, " ")},
			{Key: "msid-semantic", Value: " WMS *"},
			{Key: "extmap-allow-mixed"},
		},
	}

	for _, m := range p.Medias {
		md, err := buildMediaSection(m, p, fingerprint, setup)
		if err != nil {
			return "", err
		}
		sess.MediaDescriptions = append(sess.MediaDescriptions, md)
	}

	out, err := sess.Marshal()
	if err != nil {
		return "", fmt.Errorf("marshal answer sdp: %w", err)
	}
	return string(out), nil
}

func buildMediaSection(m *MediaAnswer, p *AnswerParams, fp mediasoup.DtlsFingerprint, setup string) (*sdp.MediaDescription, error) {
	if m.RtpParams == nil {
		return nil, fmt.Errorf("media %q missing RtpParameters", m.Mid)
	}

	protos := m.OfferProto
	if len(protos) == 0 {
		protos = []string{"UDP", "TLS", "RTP", "SAVPF"}
	}

	// Preserve offer PT order when possible, otherwise fall back to codec order.
	pts := dedupPTOrder(m.OfferPTs, m.RtpParams.Codecs)
	formats := make([]string, 0, len(pts))
	for _, pt := range pts {
		formats = append(formats, strconv.Itoa(int(pt)))
	}

	md := &sdp.MediaDescription{
		MediaName: sdp.MediaName{
			Media:   string(m.Kind),
			Port:    sdp.RangedPort{Value: 9},
			Protos:  protos,
			Formats: formats,
		},
		ConnectionInformation: &sdp.ConnectionInformation{
			NetworkType: "IN",
			AddressType: "IP4",
			Address:     &sdp.Address{Address: "0.0.0.0"},
		},
	}

	md.Attributes = append(md.Attributes,
		sdp.Attribute{Key: "rtcp", Value: "9 IN IP4 0.0.0.0"},
		sdp.Attribute{Key: "ice-ufrag", Value: p.IceParameters.UsernameFragment},
		sdp.Attribute{Key: "ice-pwd", Value: p.IceParameters.Password},
		sdp.Attribute{Key: "ice-options", Value: "trickle"},
		sdp.Attribute{Key: "fingerprint", Value: fp.Algorithm + " " + fp.Value},
		sdp.Attribute{Key: "setup", Value: setup},
		sdp.Attribute{Key: "mid", Value: m.Mid},
	)

	// ICE candidates — one line each.
	for _, c := range p.IceCandidates {
		md.Attributes = append(md.Attributes, sdp.Attribute{
			Key:   "candidate",
			Value: formatCandidate(c),
		})
	}
	md.Attributes = append(md.Attributes, sdp.Attribute{Key: "end-of-candidates"})

	md.Attributes = append(md.Attributes, sdp.Attribute{Key: string(answerDirection(m.Direction))})
	md.Attributes = append(md.Attributes, sdp.Attribute{Key: "rtcp-mux"})
	md.Attributes = append(md.Attributes, sdp.Attribute{Key: "rtcp-rsize"})

	// Header extensions.
	for _, ext := range m.RtpParams.HeaderExtensions {
		md.Attributes = append(md.Attributes, sdp.Attribute{
			Key:   "extmap",
			Value: strconv.Itoa(int(ext.Id)) + " " + ext.Uri,
		})
	}

	// Codec rtpmap / fmtp / rtcp-fb.
	codecByPT := map[uint8]*mediasoup.RtpCodecParameters{}
	for _, c := range m.RtpParams.Codecs {
		codecByPT[c.PayloadType] = c
	}
	for _, pt := range pts {
		c := codecByPT[pt]
		if c == nil {
			continue
		}
		md.Attributes = append(md.Attributes, sdp.Attribute{
			Key:   "rtpmap",
			Value: formatRtpmap(pt, c),
		})
		if fmtp := formatFmtp(c); fmtp != "" {
			md.Attributes = append(md.Attributes, sdp.Attribute{
				Key:   "fmtp",
				Value: strconv.Itoa(int(pt)) + " " + fmtp,
			})
		}
		for _, fb := range c.RtcpFeedback {
			val := strconv.Itoa(int(pt)) + " " + fb.Type
			if fb.Parameter != "" {
				val += " " + fb.Parameter
			}
			md.Attributes = append(md.Attributes, sdp.Attribute{Key: "rtcp-fb", Value: val})
		}
	}

	// WHEP-specific ssrc block (when the server is the sender).
	if m.ServerSsrc != 0 {
		cname := m.ServerCname
		if cname == "" {
			cname = "axonkit"
		}
		ssrcStr := strconv.FormatUint(uint64(m.ServerSsrc), 10)
		if m.Msid != "" {
			md.Attributes = append(md.Attributes, sdp.Attribute{Key: "msid", Value: m.Msid})
		}
		md.Attributes = append(md.Attributes,
			sdp.Attribute{Key: "ssrc", Value: ssrcStr + " cname:" + cname},
		)
		if m.Msid != "" {
			md.Attributes = append(md.Attributes,
				sdp.Attribute{Key: "ssrc", Value: ssrcStr + " msid:" + m.Msid},
			)
		}
		if m.ServerRtxSsrc != 0 {
			rtxStr := strconv.FormatUint(uint64(m.ServerRtxSsrc), 10)
			md.Attributes = append(md.Attributes,
				sdp.Attribute{Key: "ssrc", Value: rtxStr + " cname:" + cname},
				sdp.Attribute{Key: "ssrc-group", Value: "FID " + ssrcStr + " " + rtxStr},
			)
		}
	}

	return md, nil
}

// answerDirection flips the offer direction for the answer.
func answerDirection(d mediasoup.MediaDirection) mediasoup.MediaDirection {
	switch d {
	case mediasoup.MediaDirectionSendonly:
		return mediasoup.MediaDirectionRecvonly
	case mediasoup.MediaDirectionRecvonly:
		return mediasoup.MediaDirectionSendonly
	case mediasoup.MediaDirectionInactive:
		return mediasoup.MediaDirectionInactive
	default:
		return mediasoup.MediaDirectionSendrecv
	}
}

// dedupPTOrder returns PTs in offer order, filtered to those we actually know
// codec info for, dedup-ed, and if offer is empty we fall back to codec order.
func dedupPTOrder(offer []uint8, codecs []*mediasoup.RtpCodecParameters) []uint8 {
	known := map[uint8]bool{}
	for _, c := range codecs {
		known[c.PayloadType] = true
	}
	seen := map[uint8]bool{}
	var out []uint8
	for _, pt := range offer {
		if !known[pt] || seen[pt] {
			continue
		}
		seen[pt] = true
		out = append(out, pt)
	}
	if len(out) == 0 {
		for _, c := range codecs {
			if !seen[c.PayloadType] {
				seen[c.PayloadType] = true
				out = append(out, c.PayloadType)
			}
		}
	}
	return out
}

func formatCandidate(c mediasoup.IceCandidate) string {
	priority := c.Priority
	if priority == 0 {
		priority = 2130706431
	}
	typ := c.Type
	if typ == "" {
		typ = "host"
	}
	base := fmt.Sprintf("%s 1 %s %d %s %d typ %s",
		firstNonEmpty(c.Foundation, "1"),
		strings.ToUpper(string(c.Protocol)),
		priority,
		c.Address,
		c.Port,
		typ,
	)
	if c.Protocol == mediasoup.TransportProtocolTCP {
		if c.TcpType == "" {
			base += " tcptype passive"
		} else {
			base += " tcptype " + c.TcpType
		}
	}
	return base
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

func formatRtpmap(pt uint8, c *mediasoup.RtpCodecParameters) string {
	// mime is "audio/opus" -> "opus"
	codec := c.MimeType
	if slash := strings.IndexByte(codec, '/'); slash >= 0 {
		codec = codec[slash+1:]
	}
	s := strconv.Itoa(int(pt)) + " " + codec + "/" + strconv.FormatUint(uint64(c.ClockRate), 10)
	if c.Channels > 1 {
		s += "/" + strconv.FormatUint(uint64(c.Channels), 10)
	}
	return s
}

// formatFmtp emits fmtp parameters in a deterministic order (alphabetical)
// so tests are reproducible. We map back from mediasoup's strong type to the
// raw k=v form.
func formatFmtp(c *mediasoup.RtpCodecParameters) string {
	kv := map[string]string{}
	p := c.Parameters
	if p.PacketizationMode > 0 {
		kv["packetization-mode"] = strconv.FormatUint(uint64(p.PacketizationMode), 10)
	}
	if p.ProfileLevelId != "" {
		kv["profile-level-id"] = p.ProfileLevelId
	}
	if p.LevelAsymmetryAllowed > 0 {
		kv["level-asymmetry-allowed"] = strconv.FormatUint(uint64(p.LevelAsymmetryAllowed), 10)
	}
	if p.ProfileId > 0 {
		kv["profile-id"] = strconv.FormatUint(uint64(p.ProfileId), 10)
	}
	if p.Apt > 0 {
		kv["apt"] = strconv.FormatUint(uint64(p.Apt), 10)
	}
	if p.SpropStereo > 0 {
		kv["sprop-stereo"] = strconv.FormatUint(uint64(p.SpropStereo), 10)
	}
	if p.Useinbandfec > 0 {
		kv["useinbandfec"] = strconv.FormatUint(uint64(p.Useinbandfec), 10)
	}
	if p.Usedtx > 0 {
		kv["usedtx"] = strconv.FormatUint(uint64(p.Usedtx), 10)
	}
	if p.Maxplaybackrate > 0 {
		kv["maxplaybackrate"] = strconv.FormatUint(uint64(p.Maxplaybackrate), 10)
	}
	if p.Maxaveragebitrate > 0 {
		kv["maxaveragebitrate"] = strconv.FormatUint(uint64(p.Maxaveragebitrate), 10)
	}
	if p.Ptime > 0 {
		kv["ptime"] = strconv.FormatUint(uint64(p.Ptime), 10)
	}
	if p.ChannelMapping != "" {
		kv["channel_mapping"] = p.ChannelMapping
	}
	if p.NumStreams > 0 {
		kv["num_streams"] = strconv.FormatUint(uint64(p.NumStreams), 10)
	}
	if p.CoupledStreams > 0 {
		kv["coupled_streams"] = strconv.FormatUint(uint64(p.CoupledStreams), 10)
	}
	if len(kv) == 0 {
		return ""
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+kv[k])
	}
	return strings.Join(parts, ";")
}
