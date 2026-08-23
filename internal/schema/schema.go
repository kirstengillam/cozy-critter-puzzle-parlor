// Package schema defines the JSON message envelope and payload shapes
// exchanged between clients and the gateway over WebSocket. See
// IMPLEMENTATION_PLAN.md Milestone 2 for the message flows.
package schema

import "encoding/json"

// Envelope is the outer shape of every WebSocket message, in both
// directions. Payload is re-marshaled/unmarshaled based on Type.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

const (
	TypeCreateRoom  = "CREATE_ROOM"
	TypeRoomCreated = "ROOM_CREATED"
	TypeJoinRoom    = "JOIN_ROOM"
	TypeJoined      = "JOINED"
	TypeError       = "ERROR"
)

// JoinRoomRequest is the payload for a JOIN_ROOM message.
type JoinRoomRequest struct {
	PlayerID string `json:"player_id"`
	RoomCode string `json:"room_code"`
}

// RoomCreated is the payload for a ROOM_CREATED response.
type RoomCreated struct {
	RoomCode string `json:"room_code"`
}

// Joined is the payload for a JOINED response.
type Joined struct {
	RoomCode string `json:"room_code"`
	PlayerID string `json:"player_id"`
}

// ErrorPayload is the payload for an ERROR response.
type ErrorPayload struct {
	Message string `json:"message"`
}
