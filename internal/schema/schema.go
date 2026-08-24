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
	TypeCreateRoom     = "CREATE_ROOM"
	TypeRoomCreated    = "ROOM_CREATED"
	TypeJoinRoom       = "JOIN_ROOM"
	TypeJoined         = "JOINED"
	TypeMove           = "MOVE"
	TypePlayerMoved    = "PLAYER_MOVED"
	TypeChat           = "CHAT"
	TypeChatMessage    = "CHAT_MESSAGE"
	TypeChatRejected   = "CHAT_REJECTED"
	TypeStartGame      = "START_GAME"
	TypeGameStarted    = "GAME_STARTED"
	TypeGuess          = "GUESS"
	TypeGuessResult    = "GUESS_RESULT"
	TypeBalanceUpdated = "CURRENCY_BALANCE_UPDATED"
	TypeError          = "ERROR"

	TypeStartConnections   = "START_CONNECTIONS"
	TypeConnectionsStarted = "CONNECTIONS_STARTED"
	TypeConnectionsGuess   = "CONNECTIONS_GUESS"
	TypeConnectionsResult  = "CONNECTIONS_RESULT"
)

// CurrencyCritterCoins is the (currently only) in-game currency type,
// per the PRD schema.
const CurrencyCritterCoins = "CRITTER_COINS"

// JoinRoomRequest is the payload for a JOIN_ROOM message.
type JoinRoomRequest struct {
	PlayerID    string `json:"player_id"`
	RoomCode    string `json:"room_code"`
	DisplayName string `json:"display_name,omitempty"`
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
	DisplayName     string `json:"display_name,omitempty"`
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
	MessageID   string `json:"message_id"`
	Timestamp   int64  `json:"timestamp"`
	PlayerID    string `json:"player_id"`
	DisplayName string `json:"display_name,omitempty"`
	RoomID      string `json:"room_id"`
	RawText     string `json:"raw_text"`
	Status      string `json:"status"`
}

// StartGameRequest is the payload a client sends to start a word-game
// session. The word game is single-player and not room-scoped, so the
// client supplies its player_id directly rather than relying on prior
// room membership.
type StartGameRequest struct {
	PlayerID string `json:"player_id"`
}

// GameStarted is the payload for a GAME_STARTED response. The target
// word is never sent to the client.
type GameStarted struct {
	SessionID        string `json:"session_id"`
	WordLength       int    `json:"word_length"`
	GuessesRemaining int    `json:"guesses_remaining"`
}

// GuessRequest is the payload a client sends to submit a guess.
type GuessRequest struct {
	SessionID string `json:"session_id"`
	Guess     string `json:"guess"`
}

// LetterFeedback is the per-letter result of a guess (wordgame.LetterState
// values: CORRECT, PRESENT, or ABSENT).
type LetterFeedback struct {
	Letter string `json:"letter"`
	State  string `json:"state"`
}

// GuessResult is the payload for a GUESS_RESULT response.
type GuessResult struct {
	SessionID        string           `json:"session_id"`
	Guess            string           `json:"guess"`
	Feedback         []LetterFeedback `json:"feedback"`
	GuessesRemaining int              `json:"guesses_remaining"`
	Status           string           `json:"status"` // IN_PROGRESS, WON, LOST
}

// GameSessionEvent is the game-sessions Kafka message shape (per the PRD
// schema: session_id, player_id, word length, guesses, status). It's an
// audit-log stream, not consumed for broadcast — GAME_STARTED and
// GUESS_RESULT go straight back to the requesting connection instead.
// Action distinguishes SESSION_STARTED, GUESS_EVALUATED, and
// SESSION_COMPLETED (the latter only produced once, when a guess ends
// the game).
type GameSessionEvent struct {
	SessionID  string   `json:"session_id"`
	PlayerID   string   `json:"player_id"`
	Timestamp  int64    `json:"timestamp"`
	Action     string   `json:"action"`
	WordLength int      `json:"word_length"`
	Guesses    []string `json:"guesses"`
	Status     string   `json:"status"`
}

// EconomyLedgerEvent is the economy-ledger Kafka message shape, matching
// the PRD schema exactly (transaction_id, player_id, action, amount,
// currency_type, verification_hash). VerificationHash is a real
// HMAC-SHA256 over the other fields (see internal/gateway/economy.go),
// not a decorative string — the ledger consumer recomputes and checks
// it before applying anything.
type EconomyLedgerEvent struct {
	TransactionID    string `json:"transaction_id"`
	Timestamp        int64  `json:"timestamp"`
	PlayerID         string `json:"player_id"`
	GameSessionID    string `json:"game_session_id,omitempty"`
	Action           string `json:"action"` // CREDIT, DEBIT
	Amount           int    `json:"amount"`
	CurrencyType     string `json:"currency_type"`
	VerificationHash string `json:"verification_hash"`
}

// BalanceUpdated is the payload for a CURRENCY_BALANCE_UPDATED push,
// sent to a player directly (not room-scoped) after their balance
// changes.
type BalanceUpdated struct {
	PlayerID     string `json:"player_id"`
	Balance      int    `json:"balance"`
	CurrencyType string `json:"currency_type"`
}

// StartConnectionsRequest is the payload a client sends to start a
// Connections-style grouping session. Like the word game, this is
// single-player and not room-scoped — the client supplies its player_id
// directly.
type StartConnectionsRequest struct {
	PlayerID string `json:"player_id"`
}

// ConnectionsStarted is the payload for a CONNECTIONS_STARTED response:
// all 16 words, shuffled server-side. Group membership is never sent to
// the client.
type ConnectionsStarted struct {
	SessionID   string   `json:"session_id"`
	Words       []string `json:"words"`
	MaxMistakes int      `json:"max_mistakes"`
}

// ConnectionsGuessRequest is the payload a client sends to submit a
// guessed group — exactly connectionsgame.GroupSize words.
type ConnectionsGuessRequest struct {
	SessionID string   `json:"session_id"`
	Members   []string `json:"members"`
}

// ConnectionsSolvedGroup is a revealed group, sent once its 4 words have
// been correctly guessed.
type ConnectionsSolvedGroup struct {
	Level   int      `json:"level"`
	Name    string   `json:"name"`
	Members []string `json:"members"`
}

// ConnectionsResult is the payload for a CONNECTIONS_RESULT response.
// SolvedGroup is set only on a correct guess (this guess's group);
// SolvedGroups is the cumulative list of every group solved so far, so
// the client can stay in sync without tracking state itself.
type ConnectionsResult struct {
	SessionID    string                   `json:"session_id"`
	Correct      bool                     `json:"correct"`
	OneAway      bool                     `json:"one_away"`
	SolvedGroup  *ConnectionsSolvedGroup  `json:"solved_group,omitempty"`
	SolvedGroups []ConnectionsSolvedGroup `json:"solved_groups"`
	MistakesUsed int                      `json:"mistakes_used"`
	MaxMistakes  int                      `json:"max_mistakes"`
	Status       string                   `json:"status"` // IN_PROGRESS, WON, LOST
}

// ConnectionsSessionEvent is the connections-sessions Kafka message shape
// — an audit-log stream, same role as GameSessionEvent but shaped for
// the grouping game (no per-guess word list to log, just the running
// mistake count). Action distinguishes SESSION_STARTED, GUESS_EVALUATED,
// and SESSION_COMPLETED (the latter only produced once, when a guess
// ends the game).
type ConnectionsSessionEvent struct {
	SessionID    string `json:"session_id"`
	PlayerID     string `json:"player_id"`
	Timestamp    int64  `json:"timestamp"`
	Action       string `json:"action"`
	PuzzleID     int    `json:"puzzle_id"`
	MistakesUsed int    `json:"mistakes_used"`
	Status       string `json:"status"`
}
