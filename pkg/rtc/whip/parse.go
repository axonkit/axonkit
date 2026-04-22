// Package whip implements a thin SDP <-> mediasoup translation layer so that
// axonkit can accept RFC 9725 (WHIP) and WHEP requests without changing the
// mediasoup-based SFU core.
//
// The parser here only understands what a browser/OBS/ffmpeg WHIP endpoint
// typically produces: a BUNDLE-ed, rtcp-mux-ed offer with one or more
// audio/video m-sections, plain a=rtpmap/fmtp/rtcp-fb/extmap lines, ICE ufrag
// + pwd, DTLS fingerprint and (optionally) a=setup:actpass. We do not try to
// be an exhaustive SDP implementation; anything we cannot translate is
// rejected with a descriptive error so the caller can return a 4xx to the
// client.
package whip

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/pion/sdp/v3"
)

// ParsedMedia represents one m-section of a WHIP offer translated into
// mediasoup shape. For WHIP (publish) each ParsedMedia feeds a Producer; for
// WHEP (subscribe) only the Direction and negotiated header extensions /
// codecs are of interest — we keep the same struct to simplify the adapter.
type ParsedMedia struct {
	Mid            string
	Kind           mediasoup.MediaKind
	Direction      mediasoup.MediaDirection
	Msid           string
	RtpParameters  *mediasoup.RtpParameters
	Simulcast      []string // rid values in send order, empty when no simulcast
	OfferPTs       []uint8  // PTs present in the offer m= line, in order
	OfferProto     string   // e.g. UDP/TLS/RTP/SAVPF
	OfferPortProto []string
}

// ParsedOffer holds everything needed to wire a WebRtcTransport + Producers
// from an incoming WHIP offer.
type ParsedOffer struct {
	Session         *sdp.SessionDescription
	IceUfrag        string
	IcePwd          string
	DtlsSetup       string
	DtlsFingerprint mediasoup.DtlsFingerprint
	Medias          []*ParsedMedia
	BundleMids      []string
	TrickleICE      bool
	EndOfCandidates bool
}

// ParseOffer parses a WHIP/WHEP offer SDP and projects every audio/video
// m-section into mediasoup.RtpParameters, resolving codecs against the
// router's capabilities. Data channel m-sections are ignored (data is handled
// separately inside the room flow and is not in WHIP's scope).
func ParseOffer(offerSDP string, caps *mediasoup.RtpCapabilities) (*ParsedOffer, error) {
	sess := &sdp.SessionDescription{}
	if err := sess.UnmarshalString(offerSDP); err != nil {
		return nil, fmt.Errorf("sdp unmarshal: %w", err)
	}

	parsed := &ParsedOffer{Session: sess}

	// Session-level attrs (ICE, DTLS) — some endpoints put them there instead
	// of per-media.
	parsed.IceUfrag, _ = sess.Attribute("ice-ufrag")
	parsed.IcePwd, _ = sess.Attribute("ice-pwd")
	parsed.DtlsSetup, _ = sess.Attribute("setup")
	if fp, ok := sess.Attribute("fingerprint"); ok {
		parsed.DtlsFingerprint = parseFingerprint(fp)
	}
	if _, ok := sess.Attribute("ice-options"); ok {
		parsed.TrickleICE = true
	}
	if group, ok := sess.Attribute("group"); ok {
		parts := strings.Fields(group)
		if len(parts) >= 2 && strings.EqualFold(parts[0], "BUNDLE") {
			parsed.BundleMids = parts[1:]
		}
	}

	for _, md := range sess.MediaDescriptions {
		kind := mediasoup.MediaKind(md.MediaName.Media)
		if kind != mediasoup.MediaKindAudio && kind != mediasoup.MediaKindVideo {
			continue
		}

		pm := &ParsedMedia{
			Kind:           kind,
			OfferProto:     strings.Join(md.MediaName.Protos, "/"),
			OfferPortProto: md.MediaName.Protos,
		}

		if mid, ok := md.Attribute("mid"); ok {
			pm.Mid = mid
		}
		pm.Direction = extractDirection(md)
		if msid, ok := md.Attribute("msid"); ok {
			pm.Msid = msid
		}

		// ICE/DTLS overrides at media level.
		if v, ok := md.Attribute("ice-ufrag"); ok {
			parsed.IceUfrag = v
		}
		if v, ok := md.Attribute("ice-pwd"); ok {
			parsed.IcePwd = v
		}
		if v, ok := md.Attribute("setup"); ok {
			parsed.DtlsSetup = v
		}
		if fp, ok := md.Attribute("fingerprint"); ok {
			parsed.DtlsFingerprint = parseFingerprint(fp)
		}

		for _, f := range md.MediaName.Formats {
			if pt, err := strconv.ParseUint(f, 10, 8); err == nil {
				pm.OfferPTs = append(pm.OfferPTs, uint8(pt))
			}
		}

		rtp, rids, err := buildRtpParameters(md, pm.Mid, kind, caps)
		if err != nil {
			return nil, fmt.Errorf("m-section mid=%q: %w", pm.Mid, err)
		}
		pm.RtpParameters = rtp
		pm.Simulcast = rids

		parsed.Medias = append(parsed.Medias, pm)
	}

	if err := parsed.validate(); err != nil {
		return nil, err
	}
	return parsed, nil
}

func (p *ParsedOffer) validate() error {
	if p.IceUfrag == "" || p.IcePwd == "" {
		return fmt.Errorf("missing ICE ufrag/pwd in offer")
	}
	if p.DtlsFingerprint.Algorithm == "" || p.DtlsFingerprint.Value == "" {
		return fmt.Errorf("missing DTLS fingerprint in offer")
	}
	if len(p.Medias) == 0 {
		return fmt.Errorf("no audio/video m-sections in offer")
	}
	return nil
}

// parseFingerprint converts "sha-256 AB:CD:..." to a DtlsFingerprint.
func parseFingerprint(value string) mediasoup.DtlsFingerprint {
	parts := strings.Fields(value)
	if len(parts) != 2 {
		return mediasoup.DtlsFingerprint{}
	}
	return mediasoup.DtlsFingerprint{
		Algorithm: strings.ToLower(parts[0]),
		Value:     parts[1],
	}
}

func extractDirection(md *sdp.MediaDescription) mediasoup.MediaDirection {
	for _, a := range md.Attributes {
		switch a.Key {
		case "sendrecv":
			return mediasoup.MediaDirectionSendrecv
		case "sendonly":
			return mediasoup.MediaDirectionSendonly
		case "recvonly":
			return mediasoup.MediaDirectionRecvonly
		case "inactive":
			return mediasoup.MediaDirectionInactive
		}
	}
	return mediasoup.MediaDirectionSendrecv
}

// buildRtpParameters walks rtpmap/fmtp/rtcp-fb/extmap attributes of a single
// m-section and produces mediasoup.RtpParameters, negotiating codecs against
// the router's capabilities.
func buildRtpParameters(md *sdp.MediaDescription, mid string, kind mediasoup.MediaKind, caps *mediasoup.RtpCapabilities) (*mediasoup.RtpParameters, []string, error) {
	// 1. Parse rtpmap / fmtp / rtcp-fb tables keyed by PT.
	type ptInfo struct {
		mime      string
		clockRate uint32
		channels  uint8
		fmtp      map[string]string
		rawFmtp   string
		feedback  []*mediasoup.RtcpFeedback
	}
	tab := map[uint8]*ptInfo{}
	getPT := func(pt uint8) *ptInfo {
		if tab[pt] == nil {
			tab[pt] = &ptInfo{fmtp: map[string]string{}}
		}
		return tab[pt]
	}

	for _, a := range md.Attributes {
		switch a.Key {
		case "rtpmap":
			pt, rtpmap, err := parseRtpmap(a.Value)
			if err != nil {
				return nil, nil, err
			}
			info := getPT(pt)
			info.mime = string(kind) + "/" + rtpmap.codec
			info.clockRate = rtpmap.clockRate
			info.channels = rtpmap.channels
		case "fmtp":
			pt, params, raw, err := parseFmtp(a.Value)
			if err != nil {
				return nil, nil, err
			}
			info := getPT(pt)
			info.fmtp = params
			info.rawFmtp = raw
		case "rtcp-fb":
			pt, fb, err := parseRtcpFb(a.Value)
			if err != nil {
				return nil, nil, err
			}
			if pt == 0xFF { // "*" wildcard
				for p := range tab {
					tab[p].feedback = append(tab[p].feedback, fb)
				}
			} else {
				info := getPT(pt)
				info.feedback = append(info.feedback, fb)
			}
		}
	}

	// 2. Iterate offer PT order, produce RtpCodecParameters; negotiate
	//    with caps when present.
	params := &mediasoup.RtpParameters{Mid: mid}
	ptOrder := []uint8{}
	for _, f := range md.MediaName.Formats {
		n, err := strconv.ParseUint(f, 10, 8)
		if err != nil {
			continue
		}
		ptOrder = append(ptOrder, uint8(n))
	}

	for _, pt := range ptOrder {
		info := tab[pt]
		if info == nil || info.mime == "" {
			continue
		}

		codec := &mediasoup.RtpCodecParameters{
			MimeType:     info.mime,
			PayloadType:  pt,
			ClockRate:    info.clockRate,
			Channels:     info.channels,
			Parameters:   fmtpToSpecific(info.fmtp),
			RtcpFeedback: info.feedback,
		}

		if caps != nil {
			if match := matchCapability(codec, caps); match != nil {
				// Keep PT from offer; enrich feedback from caps when offer omits.
				codec.RtcpFeedback = mergeFeedback(codec.RtcpFeedback, match.RtcpFeedback)
			} else if !strings.HasSuffix(strings.ToLower(codec.MimeType), "/rtx") {
				// No capability match; skip it. Browsers will survive with remaining PTs.
				continue
			}
		}

		params.Codecs = append(params.Codecs, codec)
	}
	if len(params.Codecs) == 0 {
		return nil, nil, fmt.Errorf("no negotiable codecs for %s", kind)
	}

	// 3. Header extensions (extmap).
	for _, a := range md.Attributes {
		if a.Key != "extmap" {
			continue
		}
		id, uri, err := parseExtmap(a.Value)
		if err != nil {
			continue
		}
		params.HeaderExtensions = append(params.HeaderExtensions, &mediasoup.RtpHeaderExtensionParameters{
			Id:  id,
			Uri: uri,
		})
	}

	// 4. Encodings + RTCP + simulcast.
	rids := simulcastRids(md)
	encodings, err := buildEncodings(md, params.Codecs, rids)
	if err != nil {
		return nil, nil, err
	}
	params.Encodings = encodings

	if cname, ok := firstCNAME(md); ok {
		reduced := true
		params.Rtcp = &mediasoup.RtcpParameters{
			Cname:       cname,
			ReducedSize: &reduced,
		}
	}

	return params, rids, nil
}

type rtpmapValue struct {
	codec     string
	clockRate uint32
	channels  uint8
}

// parseRtpmap parses "96 VP8/90000" or "111 opus/48000/2".
func parseRtpmap(v string) (uint8, rtpmapValue, error) {
	ptStr, rest, ok := strings.Cut(v, " ")
	if !ok {
		return 0, rtpmapValue{}, fmt.Errorf("invalid rtpmap: %s", v)
	}
	pt, err := strconv.ParseUint(ptStr, 10, 8)
	if err != nil {
		return 0, rtpmapValue{}, fmt.Errorf("invalid rtpmap pt: %s", v)
	}
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return 0, rtpmapValue{}, fmt.Errorf("invalid rtpmap: %s", v)
	}
	rv := rtpmapValue{codec: parts[0]}
	if clock, err := strconv.ParseUint(parts[1], 10, 32); err == nil {
		rv.clockRate = uint32(clock)
	}
	if len(parts) >= 3 {
		if ch, err := strconv.ParseUint(parts[2], 10, 8); err == nil {
			rv.channels = uint8(ch)
		}
	}
	return uint8(pt), rv, nil
}

// parseFmtp parses "96 apt=95" or "111 minptime=10;useinbandfec=1".
func parseFmtp(v string) (uint8, map[string]string, string, error) {
	ptStr, rest, ok := strings.Cut(v, " ")
	if !ok {
		return 0, nil, "", fmt.Errorf("invalid fmtp: %s", v)
	}
	pt, err := strconv.ParseUint(ptStr, 10, 8)
	if err != nil {
		return 0, nil, "", fmt.Errorf("invalid fmtp pt: %s", v)
	}
	params := map[string]string{}
	for _, kv := range strings.Split(rest, ";") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		key, val, hasEq := strings.Cut(kv, "=")
		if !hasEq {
			params[strings.TrimSpace(key)] = ""
		} else {
			params[strings.TrimSpace(key)] = strings.TrimSpace(val)
		}
	}
	return uint8(pt), params, rest, nil
}

// parseRtcpFb parses "111 transport-cc" or "* goog-remb". "*" returns 0xFF.
func parseRtcpFb(v string) (uint8, *mediasoup.RtcpFeedback, error) {
	ptStr, rest, ok := strings.Cut(v, " ")
	if !ok {
		return 0, nil, fmt.Errorf("invalid rtcp-fb: %s", v)
	}
	fb := &mediasoup.RtcpFeedback{}
	if typ, param, hasParam := strings.Cut(rest, " "); hasParam {
		fb.Type = typ
		fb.Parameter = param
	} else {
		fb.Type = rest
	}
	if ptStr == "*" {
		return 0xFF, fb, nil
	}
	pt, err := strconv.ParseUint(ptStr, 10, 8)
	if err != nil {
		return 0, nil, fmt.Errorf("invalid rtcp-fb pt: %s", v)
	}
	return uint8(pt), fb, nil
}

// parseExtmap parses "3 urn:ietf:params:rtp-hdrext:sdes:mid".
func parseExtmap(v string) (uint8, string, error) {
	idStr, rest, ok := strings.Cut(v, " ")
	if !ok {
		return 0, "", fmt.Errorf("invalid extmap: %s", v)
	}
	// id may have "/sendonly" etc suffix.
	if slash := strings.IndexByte(idStr, '/'); slash >= 0 {
		idStr = idStr[:slash]
	}
	id, err := strconv.ParseUint(idStr, 10, 8)
	if err != nil {
		return 0, "", fmt.Errorf("invalid extmap id: %s", v)
	}
	// Strip optional trailing extension attributes.
	uri := rest
	if sp := strings.IndexByte(uri, ' '); sp >= 0 {
		uri = uri[:sp]
	}
	return uint8(id), uri, nil
}

func simulcastRids(md *sdp.MediaDescription) []string {
	sc, ok := md.Attribute("simulcast")
	if !ok {
		return nil
	}
	// "simulcast:send q;h;f" or "send q;h;f"
	body := strings.TrimSpace(sc)
	if strings.HasPrefix(body, "send ") {
		body = strings.TrimPrefix(body, "send ")
	} else if strings.HasPrefix(body, "recv ") {
		body = strings.TrimPrefix(body, "recv ")
	}
	var rids []string
	for _, alt := range strings.Split(body, ";") {
		alt = strings.TrimSpace(alt)
		if alt == "" {
			continue
		}
		// Strip optional leading ~ (paused).
		alt = strings.TrimPrefix(alt, "~")
		// Comma-separated alt list picks the primary.
		if idx := strings.IndexByte(alt, ','); idx >= 0 {
			alt = alt[:idx]
		}
		rids = append(rids, alt)
	}
	return rids
}

// buildEncodings derives encodings from ssrc / ssrc-group / rid attributes.
// For non-simulcast publish, we assign a single encoding with the primary ssrc
// and optional RTX ssrc. For simulcast, we use rid-based encodings.
func buildEncodings(md *sdp.MediaDescription, codecs []*mediasoup.RtpCodecParameters, rids []string) ([]*mediasoup.RtpEncodingParameters, error) {
	// Find primary media codec PT and associated RTX PT (if any).
	primaryCodec := firstMediaCodec(codecs)
	rtxPt := findRtxPt(codecs, primaryCodec)

	// Simulcast: one encoding per rid.
	if len(rids) > 0 {
		enc := make([]*mediasoup.RtpEncodingParameters, 0, len(rids))
		for _, r := range rids {
			e := &mediasoup.RtpEncodingParameters{Rid: r}
			if rtxPt != 0 {
				e.Rtx = &mediasoup.RtpEncodingRtx{Ssrc: 0} // mediasoup fills in
			}
			enc = append(enc, e)
		}
		return enc, nil
	}

	// Single stream: collect ssrc + ssrc-group FID.
	ssrcs := collectSsrcs(md)
	if len(ssrcs) == 0 {
		// WHEP (recvonly) doesn't carry ssrcs in the offer — mediasoup Consume
		// generates them. Return an empty encodings slice.
		return nil, nil
	}

	rtxPair := rtxPairs(md)
	used := map[uint32]bool{}
	var encodings []*mediasoup.RtpEncodingParameters
	for _, ssrc := range ssrcs {
		if used[ssrc] {
			continue
		}
		// Skip RTX-only ssrcs (second of an FID pair).
		if _, isRtx := rtxPair[ssrc]; isRtx {
			continue
		}
		e := &mediasoup.RtpEncodingParameters{Ssrc: ssrc}
		if pair, ok := rtxPair[0]; ok && len(pair) > 0 {
			// Fallback when layout is unusual.
			_ = pair
		}
		if rtxSsrc, ok := findRtxFor(md, ssrc); ok && rtxPt != 0 {
			e.Rtx = &mediasoup.RtpEncodingRtx{Ssrc: rtxSsrc}
			used[rtxSsrc] = true
		}
		used[ssrc] = true
		encodings = append(encodings, e)
	}

	_ = primaryCodec
	return encodings, nil
}

func isRtxMime(mime string) bool {
	return strings.HasSuffix(strings.ToLower(mime), "/rtx")
}

func firstMediaCodec(codecs []*mediasoup.RtpCodecParameters) *mediasoup.RtpCodecParameters {
	for _, c := range codecs {
		if isRtxMime(c.MimeType) {
			continue
		}
		return c
	}
	if len(codecs) > 0 {
		return codecs[0]
	}
	return nil
}

func findRtxPt(codecs []*mediasoup.RtpCodecParameters, primary *mediasoup.RtpCodecParameters) uint8 {
	if primary == nil {
		return 0
	}
	for _, c := range codecs {
		if !isRtxMime(c.MimeType) {
			continue
		}
		if c.Parameters.Apt == primary.PayloadType {
			return c.PayloadType
		}
	}
	return 0
}

// collectSsrcs enumerates a=ssrc lines in declaration order, deduped.
func collectSsrcs(md *sdp.MediaDescription) []uint32 {
	seen := map[uint32]bool{}
	var out []uint32
	for _, a := range md.Attributes {
		if a.Key != "ssrc" {
			continue
		}
		head, _, _ := strings.Cut(a.Value, " ")
		n, err := strconv.ParseUint(head, 10, 32)
		if err != nil {
			continue
		}
		ssrc := uint32(n)
		if seen[ssrc] {
			continue
		}
		seen[ssrc] = true
		out = append(out, ssrc)
	}
	return out
}

// rtxPairs returns a set of RTX ssrcs (the "rtx" element of each FID group).
func rtxPairs(md *sdp.MediaDescription) map[uint32][]uint32 {
	out := map[uint32][]uint32{}
	for _, a := range md.Attributes {
		if a.Key != "ssrc-group" {
			continue
		}
		parts := strings.Fields(a.Value)
		if len(parts) >= 3 && strings.EqualFold(parts[0], "FID") {
			primary, err1 := strconv.ParseUint(parts[1], 10, 32)
			rtx, err2 := strconv.ParseUint(parts[2], 10, 32)
			if err1 != nil || err2 != nil {
				continue
			}
			out[uint32(rtx)] = []uint32{uint32(primary)}
		}
	}
	return out
}

func findRtxFor(md *sdp.MediaDescription, primary uint32) (uint32, bool) {
	for _, a := range md.Attributes {
		if a.Key != "ssrc-group" {
			continue
		}
		parts := strings.Fields(a.Value)
		if len(parts) >= 3 && strings.EqualFold(parts[0], "FID") {
			p, err1 := strconv.ParseUint(parts[1], 10, 32)
			r, err2 := strconv.ParseUint(parts[2], 10, 32)
			if err1 != nil || err2 != nil {
				continue
			}
			if uint32(p) == primary { //nolint:gocritic
				return uint32(r), true
			}
		}
	}
	return 0, false
}

func firstCNAME(md *sdp.MediaDescription) (string, bool) {
	for _, a := range md.Attributes {
		if a.Key != "ssrc" {
			continue
		}
		// "12345 cname:abcd"
		parts := strings.SplitN(a.Value, " ", 2)
		if len(parts) < 2 {
			continue
		}
		kv := strings.TrimSpace(parts[1])
		if strings.HasPrefix(kv, "cname:") {
			return strings.TrimPrefix(kv, "cname:"), true
		}
	}
	return "", false
}

// matchCapability returns the capability that matches the given codec on mime,
// clock rate, channels and (for H.264/VP9) codec-specific params.
func matchCapability(codec *mediasoup.RtpCodecParameters, caps *mediasoup.RtpCapabilities) *mediasoup.RtpCodecCapability {
	for _, cap := range caps.Codecs {
		if !strings.EqualFold(cap.MimeType, codec.MimeType) {
			continue
		}
		if cap.ClockRate != codec.ClockRate {
			continue
		}
		if cap.Channels != 0 && codec.Channels != 0 && cap.Channels != codec.Channels {
			continue
		}
		if !codecParamsCompatible(codec, cap) {
			continue
		}
		return cap
	}
	return nil
}

func codecParamsCompatible(codec *mediasoup.RtpCodecParameters, cap *mediasoup.RtpCodecCapability) bool {
	mime := strings.ToLower(codec.MimeType)
	switch {
	case strings.HasSuffix(mime, "/h264"):
		if codec.Parameters.PacketizationMode != cap.Parameters.PacketizationMode {
			return false
		}
		// RFC 6184 §8.2.2: profile-level-id identifies a profile *and* a level,
		// but negotiation only requires the profile to match — the level is
		// taken to be the min of the two sides. Doing a literal string compare
		// (as we did before) rejects perfectly valid offers like ffmpeg's
		// `profile-level-id=424016` (Constrained Baseline, Level 2.2) against
		// our caps' `42e01f` (Constrained Baseline, Level 3.1), which are the
		// same profile. Fall back to the strict compare only when we cannot
		// decode either side.
		if !h264ProfileCompatible(codec.Parameters.ProfileLevelId, cap.Parameters.ProfileLevelId) {
			return false
		}
	case strings.HasSuffix(mime, "/vp9"):
		if cap.Parameters.ProfileId != 0 && codec.Parameters.ProfileId != 0 &&
			cap.Parameters.ProfileId != codec.Parameters.ProfileId {
			return false
		}
	}
	return true
}

// h264ProfileCompatible reports whether two H.264 `profile-level-id` strings
// describe the same profile (independent of level). Empty strings on either
// side are treated as "unknown ⇒ compatible", matching the prior behavior.
func h264ProfileCompatible(a, b string) bool {
	if a == "" || b == "" {
		return true
	}
	pa, ioa, ok1 := parseH264ProfileIdc(a)
	pb, iob, ok2 := parseH264ProfileIdc(b)
	if !ok1 || !ok2 {
		// Undecodable — be conservative and fall back to the literal compare.
		return strings.EqualFold(a, b)
	}
	return h264ProfileName(pa, ioa) == h264ProfileName(pb, iob)
}

// parseH264ProfileIdc decodes the first two bytes (profile_idc, profile_iop)
// of a 6-hex-digit profile-level-id. Returns ok=false on any malformed input.
func parseH264ProfileIdc(pli string) (idc, iop byte, ok bool) {
	if len(pli) != 6 {
		return 0, 0, false
	}
	b, err := hex.DecodeString(pli)
	if err != nil || len(b) != 3 {
		return 0, 0, false
	}
	return b[0], b[1], true
}

// h264ProfileName maps (profile_idc, profile_iop) to a canonical profile
// identifier per RFC 6184 Annex A, matching libwebrtc's `H264Profile` enum for
// the profiles WebRTC actually negotiates. Unknown profile_idc values collapse
// to a distinct bucket so they only match themselves.
func h264ProfileName(idc, iop byte) string {
	// Bit layout of profile_iop (MSB first):
	//   cs0 cs1 cs2 cs3 cs4 cs5 0 0
	cs1 := iop&0x40 != 0
	cs4 := iop&0x08 != 0
	cs5 := iop&0x04 != 0
	switch idc {
	case 0x42, 0x4d: // Baseline / Main — constraint_set1_flag promotes to Constrained Baseline.
		if cs1 {
			return "ConstrainedBaseline"
		}
		if idc == 0x42 {
			return "Baseline"
		}
		return "Main"
	case 0x58:
		return "Extended"
	case 0x64: // High — cs4 && cs5 promote to Constrained High.
		if cs4 && cs5 {
			return "ConstrainedHigh"
		}
		return "High"
	case 0x6e:
		return "High10"
	case 0x7a:
		return "High422"
	case 0xf4:
		return "High444Predictive"
	}
	return fmt.Sprintf("idc=0x%02x", idc)
}

func mergeFeedback(existing, extra []*mediasoup.RtcpFeedback) []*mediasoup.RtcpFeedback {
	if len(existing) == 0 {
		out := make([]*mediasoup.RtcpFeedback, 0, len(extra))
		for _, f := range extra {
			out = append(out, &mediasoup.RtcpFeedback{Type: f.Type, Parameter: f.Parameter})
		}
		return out
	}
	return existing
}

// fmtpToSpecific maps parsed fmtp k=v pairs into mediasoup's strongly typed
// RtpCodecSpecificParameters. Unknown keys are dropped (mediasoup ignores
// unknown params anyway).
func fmtpToSpecific(kv map[string]string) mediasoup.RtpCodecSpecificParameters {
	p := mediasoup.RtpCodecSpecificParameters{}
	getU32 := func(k string) uint32 {
		if s, ok := kv[k]; ok {
			if n, err := strconv.ParseUint(s, 10, 32); err == nil {
				return uint32(n)
			}
		}
		return 0
	}
	getU8 := func(k string) uint8 {
		if s, ok := kv[k]; ok {
			if n, err := strconv.ParseUint(s, 10, 8); err == nil {
				return uint8(n)
			}
		}
		return 0
	}
	p.PacketizationMode = getU32("packetization-mode")
	p.ProfileLevelId = kv["profile-level-id"]
	p.LevelAsymmetryAllowed = getU32("level-asymmetry-allowed")
	p.ProfileId = getU32("profile-id")
	p.Apt = getU8("apt")
	p.SpropStereo = getU8("sprop-stereo")
	p.Useinbandfec = getU8("useinbandfec")
	p.Usedtx = getU8("usedtx")
	p.Maxplaybackrate = getU32("maxplaybackrate")
	p.Maxaveragebitrate = getU32("maxaveragebitrate")
	p.Ptime = getU32("ptime")
	p.ChannelMapping = kv["channel_mapping"]
	p.NumStreams = getU32("num_streams")
	p.CoupledStreams = getU32("coupled_streams")
	p.XGoogleStartBitrate = getU32("x-google-start-bitrate")
	p.XGoogleMaxBitrate = getU32("x-google-max-bitrate")
	p.XGoogleMinBitrate = getU32("x-google-min-bitrate")
	return p
}
