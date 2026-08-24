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
	TypeCreateRoom   = "CREATE_ROOM"
	TypeRoomCreated  = "ROOM_CREATED"
	TypeJoinRoom     = "JOIN_ROOM"
	TypeJoined       = "JOINED"
	TypeMove         = "MOVE"
	TypePlayerMoved  = "PLAYER_MOVED"
	TypeChat         = "CHAT"
	TypeChatMessage  = "CHAT_MESSAGE"
	TypeChatRejected = "CHAT_REJECTED"
	TypeError        = "ERROR"
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

// MoveRequest is the payload a client sends over WS to move; the server
// fills in everything else (current position, event id, timestamp) to
// build the full PlayerPositionEvent.
type MoveRequest struct {
	TargetX         int    `json:"target_x"`
	TargetY         int    `json:"target_y"`
	FacingDirection string `json:"facing_direction"`
}

// PlayerPositionEvent is both the player-positions Kafka message shape
// (per the PRD schema) and the PLAYER_MOVED broadcast payload sent to
// every connection in the room.
type PlayerPositionEvent struct {
	EventID         string `json:"event_id"`
	Timestamp       int64  `json:"timestamp"`
	PlayerID        string `json:"player_id"`
	RoomID          string `json:"room_id"`
	Action          string `json:"action"`
	CurrentX        int    `json:"current_x"`
	CurrentY        int    `json:"current_y"`
	TargetX         int    `json:"target_x"`
	TargetY         int    `json:"target_y"`
	FacingDirection string `json:"facing_direction"`
}

// ChatRequest is the payload a client sends over WS to chat; the server
// fills in everything else to build the full ChatMessageEvent.
type ChatRequest struct {
	Text string `json:"text"`
}

// ChatMessageEvent is both the chat-messages Kafka message shape (per the
// PRD schema) and the CHAT_MESSAGE/CHAT_REJECTED payload sent back over
// WS — Status distinguishes PENDING_VALIDATION (never seen by clients),
// APPROVED (broadcast to the room), and REJECTED (sent only to the
// sender).
type ChatMessageEvent struct {
	MessageID string `json:"message_id"`
	Timestamp int64  `json:"timestamp"`
	PlayerID  string `json:"player_id"`
	RoomID    string `json:"room_id"`
	RawText   string `json:"raw_text"`
	Status    string `json:"status"`
}
