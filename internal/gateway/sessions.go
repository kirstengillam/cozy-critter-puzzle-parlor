package gateway

import "sync"

const (
	statusInProgress = "IN_PROGRESS"
	statusWon        = "WON"
	statusLost       = "LOST"
)

// gameSession is one player's in-progress or finished word-game round.
// Ephemeral, like everything else in Milestone 3-4 — see
// IMPLEMENTATION_PLAN.md's "Player identity is ephemeral for now".
type gameSession struct {
	PlayerID string
	Target   string
	Guesses  []string
	Status   string
}

// sessionStore tracks in-flight and completed word-game sessions,
// keyed by session id.
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]*gameSession
}

func newSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]*gameSession)}
}

func (s *sessionStore) create(sessionID, playerID, target string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = &gameSession{
		PlayerID: playerID,
		Target:   target,
		Status:   statusInProgress,
	}
}

// get returns a copy of the session's current state, safe to read
// without holding the store's lock.
func (s *sessionStore) get(sessionID string) (gameSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return gameSession{}, false
	}
	return *sess, true
}

// recordGuess appends guess to the session, updates its status, and
// returns the updated state. Reports false if the session doesn't
// exist.
func (s *sessionStore) recordGuess(sessionID, guess, status string) (gameSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return gameSession{}, false
	}
	sess.Guesses = append(sess.Guesses, guess)
	sess.Status = status
	return *sess, true
}
