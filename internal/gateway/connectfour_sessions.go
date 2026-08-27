package gateway

import (
	"sync"

	"cozy-critter-puzzle-parlor/internal/connectfour"
)

const (
	statusWaiting   = "WAITING"
	statusDraw      = "DRAW"
	statusAbandoned = "ABANDONED"
)

// connectFourSession is one Connect Four match: two players sharing a
// single board, unlike gameSession/connectionsSession which each belong
// to exactly one player. Player2ID is empty while Status is
// statusWaiting.
type connectFourSession struct {
	SessionID   string
	RoomCode    string
	Player1ID   string
	Player2ID   string
	Board       connectfour.Board
	CurrentTurn string
	Status      string
	WinnerID    string
}

// connectFourStore tracks in-flight and completed Connect Four matches.
// Unlike sessionStore/connectionsSessionStore (looked up only by a
// client-supplied session id, since each session belongs to exactly one
// player), pairing needs two more indexes: pendingByRoom finds the match
// a room's table is currently waiting to fill, and sessionByPlayer finds
// a player's current match for idempotent re-arrival and disconnect
// cleanup.
type connectFourStore struct {
	mu              sync.Mutex
	sessions        map[string]*connectFourSession // session id -> session
	pendingByRoom   map[string]string              // room code -> session id, only while WAITING
	sessionByPlayer map[string]string              // player id -> session id, cleared once the match ends
}

func newConnectFourStore() *connectFourStore {
	return &connectFourStore{
		sessions:        make(map[string]*connectFourSession),
		pendingByRoom:   make(map[string]string),
		sessionByPlayer: make(map[string]string),
	}
}

// sessionForPlayer returns playerID's current session (waiting or
// in-progress), if any — used to make re-arriving at the table
// idempotent instead of creating a duplicate/conflicting session.
func (s *connectFourStore) sessionForPlayer(playerID string) (connectFourSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.sessionByPlayer[playerID]
	if !ok {
		return connectFourSession{}, false
	}
	return *s.sessions[id], true
}

// pendingInRoom returns the session currently waiting for a second
// player at roomCode's table, if any.
func (s *connectFourStore) pendingInRoom(roomCode string) (connectFourSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.pendingByRoom[roomCode]
	if !ok {
		return connectFourSession{}, false
	}
	return *s.sessions[id], true
}

// createWaiting starts a new WAITING session for playerID at roomCode's
// table.
func (s *connectFourStore) createWaiting(sessionID, roomCode, playerID string) connectFourSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := &connectFourSession{
		SessionID: sessionID,
		RoomCode:  roomCode,
		Player1ID: playerID,
		Board:     connectfour.NewBoard(),
		Status:    statusWaiting,
	}
	s.sessions[sessionID] = sess
	s.pendingByRoom[roomCode] = sessionID
	s.sessionByPlayer[playerID] = sessionID
	return *sess
}

// pair fills a WAITING session's second-player slot, starts play with
// Player1 going first, and stops tracking the room as pending — a third
// arrival at the table starts a fresh wait rather than joining this
// match, an accepted simplification at this project's scale (no
// spectator/exclusivity logic).
func (s *connectFourStore) pair(sessionID, player2ID string) (connectFourSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return connectFourSession{}, false
	}
	sess.Player2ID = player2ID
	sess.CurrentTurn = sess.Player1ID
	sess.Status = statusInProgress
	delete(s.pendingByRoom, sess.RoomCode)
	s.sessionByPlayer[player2ID] = sessionID
	return *sess, true
}

// get returns a copy of the session's current state, safe to read
// without holding the store's lock.
func (s *connectFourStore) get(sessionID string) (connectFourSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return connectFourSession{}, false
	}
	return *sess, true
}

// recordMove applies a move's outcome and returns the updated state.
// Once status leaves IN_PROGRESS (a win, a draw, or an abandonment),
// both players are dropped from sessionByPlayer so either is free to
// start a fresh match. Reports false if the session doesn't exist.
func (s *connectFourStore) recordMove(sessionID string, board connectfour.Board, nextTurn, status, winnerID string) (connectFourSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return connectFourSession{}, false
	}
	sess.Board = board
	sess.CurrentTurn = nextTurn
	sess.Status = status
	sess.WinnerID = winnerID
	if status != statusInProgress {
		delete(s.sessionByPlayer, sess.Player1ID)
		delete(s.sessionByPlayer, sess.Player2ID)
	}
	return *sess, true
}

// abandon removes BOTH playerID and its opponent from sessionByPlayer
// (the session itself, and its Status, are left alone here — that's
// recordMove's job) and reports the session plus the opponent's id, if
// playerID had a still-connected opponent. A WAITING session with no
// opponent yet is discarded outright (nobody to notify). Removing the
// opponent from sessionByPlayer immediately, before the caller's
// follow-up recordMove(..., statusAbandoned, ...) call lands, closes a
// race where the opponent could re-trigger START_CONNECT_FOUR in that
// gap and get paired into a brand new session that then gets orphaned
// once the abandonment finishes processing. Reports hadSession=false if
// playerID had no session at all, including one that already finished
// (recordMove already cleared it).
func (s *connectFourStore) abandon(playerID string) (sess connectFourSession, opponentID string, hadSession bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sessionID, ok := s.sessionByPlayer[playerID]
	if !ok {
		return connectFourSession{}, "", false
	}
	found := s.sessions[sessionID]
	delete(s.sessionByPlayer, playerID)

	if found.Status == statusWaiting {
		delete(s.pendingByRoom, found.RoomCode)
		delete(s.sessions, sessionID)
		return *found, "", false
	}

	opponentID = found.Player1ID
	if playerID == found.Player1ID {
		opponentID = found.Player2ID
	}
	delete(s.sessionByPlayer, opponentID)
	return *found, opponentID, true
}
