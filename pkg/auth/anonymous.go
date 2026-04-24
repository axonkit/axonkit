package auth

import "github.com/axonkit/axonkit/pkg/proto"

// AnonymousClaims returns a permissive Claims payload scoped to roomName
// for an anonymous caller identified by identity. It is used by every
// ingest/admin path that falls through to anonymous mode when the
// operator enables Auth.AllowAnonymous: HTTP admin APIs, WHIP/WHEP
// publish/subscribe, and RTMP push ingest.
//
// The caller is expected to supply an identity that is both unique
// (so participants do not collide inside a room) and recognizable by
// prefix so logs make clear which surface the anonymous user came in
// through. The conventional prefixes are:
//
//   - "whip_<id>" for WHIP / WHEP anonymous sessions,
//   - "rtmp_<id>" for RTMP push anonymous sessions, and
//   - "anon_<id>" for administrative HTTP anonymous callers.
//
// The returned grant carries Room.Admin=true plus publish + subscribe
// media grants because that is what every current caller needs. The
// RoomGrant.Name is set to roomName so downstream authorization can
// still scope the claim to one room.
func AnonymousClaims(identity, roomName string) *Claims {
	boolTrue := true
	return &Claims{
		Sub:  identity,
		Name: identity,
		Grants: &proto.ClaimGrants{
			Identity: identity,
			Room:     &proto.RoomGrant{Name: roomName, Join: true, Admin: true},
			Media: &proto.MediaGrant{
				CanPublish:   &boolTrue,
				CanSubscribe: &boolTrue,
			},
		},
	}
}
