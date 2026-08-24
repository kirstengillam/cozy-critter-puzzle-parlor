package gateway

import (
	"testing"

	"cozy-critter-puzzle-parlor/internal/schema"
)

func TestEconomyStoreCredit(t *testing.T) {
	e := newEconomyStore()

	balance, err := e.apply("CREDIT", "capy", 25)
	if err != nil {
		t.Fatalf("apply CREDIT: %v", err)
	}
	if balance != 25 {
		t.Fatalf("balance = %d, want 25", balance)
	}

	balance, err = e.apply("CREDIT", "capy", 15)
	if err != nil {
		t.Fatalf("apply CREDIT: %v", err)
	}
	if balance != 40 {
		t.Fatalf("balance = %d, want 40", balance)
	}
}

func TestEconomyStoreDebit(t *testing.T) {
	e := newEconomyStore()
	if _, err := e.apply("CREDIT", "capy", 30); err != nil {
		t.Fatalf("apply CREDIT: %v", err)
	}

	balance, err := e.apply("DEBIT", "capy", 10)
	if err != nil {
		t.Fatalf("apply DEBIT: %v", err)
	}
	if balance != 20 {
		t.Fatalf("balance = %d, want 20", balance)
	}
}

func TestEconomyStoreDebitInsufficientBalance(t *testing.T) {
	e := newEconomyStore()
	if _, err := e.apply("CREDIT", "capy", 5); err != nil {
		t.Fatalf("apply CREDIT: %v", err)
	}

	balance, err := e.apply("DEBIT", "capy", 10)
	if err == nil {
		t.Fatal("apply DEBIT over balance: got nil error, want an error")
	}
	if balance != 5 {
		t.Fatalf("balance after rejected DEBIT = %d, want unchanged 5", balance)
	}
}

func TestEconomyStoreUnknownAction(t *testing.T) {
	e := newEconomyStore()
	if _, err := e.apply("YEET", "capy", 10); err == nil {
		t.Fatal("apply with unknown action: got nil error, want an error")
	}
}

func TestSignAndVerifyLedgerEvent(t *testing.T) {
	secret := []byte("test-secret")
	evt := schema.EconomyLedgerEvent{
		TransactionID: "tx1",
		Timestamp:     123456,
		PlayerID:      "capy",
		GameSessionID: "sess1",
		Action:        "CREDIT",
		Amount:        50,
		CurrencyType:  schema.CurrencyCritterCoins,
	}
	evt.VerificationHash = signLedgerEvent(secret, evt)

	if !verifyLedgerEvent(secret, evt) {
		t.Fatal("verifyLedgerEvent: freshly signed event failed verification")
	}

	tampered := evt
	tampered.Amount = 5000
	if verifyLedgerEvent(secret, tampered) {
		t.Fatal("verifyLedgerEvent: tampered amount passed verification")
	}

	if verifyLedgerEvent([]byte("wrong-secret"), evt) {
		t.Fatal("verifyLedgerEvent: wrong secret passed verification")
	}
}
