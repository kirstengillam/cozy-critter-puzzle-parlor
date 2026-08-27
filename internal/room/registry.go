// Package room manages the gateway's in-memory room registry. Rooms are
// private and code-joined, not publicly matchmade — see
// IMPLEMENTATION_PLAN.md's "Locked-in decisions" for why — and ephemeral:
// nothing here is persisted.
package room

import (
	"crypto/rand"
	"fmt"
	"sync"
)

// codeAlphabet excludes visually ambiguous characters (0/O, 1/I).
const codeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

const codeLength = 6

const maxCreateAttempts = 10

// Registry tracks which room codes currently exist. It intentionally
// knows nothing about connections or members yet — that arrives with
// broadcast in Milestone 2's movement steps.
type Registry struct {
	mu    sync.Mutex
	codes map[string]struct{}
}

func NewRegistry() *Registry {
	return &Registry{codes: make(map[string]struct{})}
}

// Create generates a fresh, unique room code and registers it.
func (reg *Registry) Create() (string, error) {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	for i := 0; i < maxCreateAttempts; i++ {
		code, err := randomCode()
		if err != nil {
			return "", err
		}
		if _, exists := reg.codes[code]; exists {
			continue
		}
		reg.codes[code] = struct{}{}
		return code, nil
	}
	return "", fmt.Errorf("room: could not generate a unique code after %d attempts", maxCreateAttempts)
}

// Exists reports whether a room code is currently registered.
func (reg *Registry) Exists(code string) bool {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	_, ok := reg.codes[code]
	return ok
}

// Delete reclaims a room code, e.g. once its last player disconnects, so
// it stops existing (JoinRoom will reject it) and its code can be
// generated again by a future Create instead of leaking forever.
func (reg *Registry) Delete(code string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.codes, code)
}

func randomCode() (string, error) {
	raw := make([]byte, codeLength)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	out := make([]byte, codeLength)
	for i, b := range raw {
		out[i] = codeAlphabet[int(b)%len(codeAlphabet)]
	}
	return string(out), nil
}
