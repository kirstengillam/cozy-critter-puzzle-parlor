package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"cozy-critter-puzzle-parlor/internal/schema"
)

// economyStore tracks each player's in-memory currency balance.
// Ephemeral, like everything else in Milestone 3-4 — see
// IMPLEMENTATION_PLAN.md's "Player identity is ephemeral for now".
type economyStore struct {
	mu       sync.Mutex
	balances map[string]int
}

func newEconomyStore() *economyStore {
	return &economyStore{balances: make(map[string]int)}
}

// apply adds (CREDIT) or subtracts (DEBIT) amount from playerID's
// balance and returns the new balance. DEBIT is rejected if it would
// take the balance negative.
func (e *economyStore) apply(action, playerID string, amount int) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	balance := e.balances[playerID]
	switch action {
	case "CREDIT":
		balance += amount
	case "DEBIT":
		if balance < amount {
			return balance, errors.New("insufficient balance")
		}
		balance -= amount
	default:
		return balance, fmt.Errorf("unknown ledger action %q", action)
	}
	e.balances[playerID] = balance
	return balance, nil
}

// signLedgerEvent computes an HMAC-SHA256 over evt's fields, keyed by
// secret — a real integrity check (not a decorative string), so a
// tampered or forged ledger entry can be told apart from one this
// gateway actually produced. See verifyLedgerEvent.
func signLedgerEvent(secret []byte, evt schema.EconomyLedgerEvent) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%s|%s|%s|%s|%d|%s|%d",
		evt.TransactionID, evt.PlayerID, evt.GameSessionID, evt.Action, evt.Amount, evt.CurrencyType, evt.Timestamp)
	return hex.EncodeToString(mac.Sum(nil))
}

// verifyLedgerEvent reports whether evt.VerificationHash matches what
// signLedgerEvent would compute for it.
func verifyLedgerEvent(secret []byte, evt schema.EconomyLedgerEvent) bool {
	want := signLedgerEvent(secret, evt)
	got := evt.VerificationHash
	return hmac.Equal([]byte(want), []byte(got))
}
