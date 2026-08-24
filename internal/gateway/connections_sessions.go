package gateway

import (
	"sync"

	"cozy-critter-puzzle-parlor/internal/connectionsgame"
)

// connectionsSession is one player's in-progress or finished
// Connections-style grouping round. Ephemeral, same as gameSession — see
// IMPLEMENTATION_PLAN.md's "Player identity is ephemeral for now".
type connectionsSession struct {
	PlayerID     string
	Puzzle       connectionsgame.Puzzle
	SolvedGroups map[int]bool
	MistakesUsed int
	Status       string
}

// connectionsSessionStore tracks in-flight and completed Connections
// sessions, keyed by session id.
type connectionsSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*connectionsSession
}

func newConnectionsSessionStore() *connectionsSessionStore {
	return &connectionsSessionStore{sessions: make(map[string]*connectionsSession)}
}

func (s *connectionsSessionStore) create(sessionID, playerID string, puzzle connectionsgame.Puzzle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = &connectionsSession{
		PlayerID:     playerID,
		Puzzle:       puzzle,
		SolvedGroups: make(map[int]bool),
		Status:       statusInProgress,
	}
}

// get returns a copy of the session's current state, safe to read
// without holding the store's lock. SolvedGroups is shared (a map), so
// callers must treat it as read-only.
func (s *connectionsSessionStore) get(sessionID string) (connectionsSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return connectionsSession{}, false
	}
	return *sess, true
}

// recordGuess updates the session for one guess outcome and returns the
// updated state. groupIndex is the matched group's index into
// Puzzle.Answers (marks it solved) — NOT its Level, which isn't unique
// across a puzzle's 4 groups for newer archive entries that lost their
// level data (see connectionsgame.Group's doc comment) — or -1 for a
// wrong guess (increments MistakesUsed). Reports false if the session
// doesn't exist.
func (s *connectionsSessionStore) recordGuess(sessionID string, groupIndex int, status string) (connectionsSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return connectionsSession{}, false
	}
	if groupIndex >= 0 {
		sess.SolvedGroups[groupIndex] = true
	} else {
		sess.MistakesUsed++
	}
	sess.Status = status
	return *sess, true
}
