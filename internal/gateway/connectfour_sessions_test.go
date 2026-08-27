package gateway

import "testing"

func TestConnectFourCreateWaitingThenPair(t *testing.T) {
	s := newConnectFourStore()

	waiting := s.createWaiting("sess-1", "ROOM01", "alice")
	if waiting.Status != statusWaiting || waiting.Player1ID != "alice" || waiting.Player2ID != "" {
		t.Fatalf("createWaiting: unexpected session %+v", waiting)
	}

	pending, ok := s.pendingInRoom("ROOM01")
	if !ok || pending.SessionID != "sess-1" {
		t.Fatalf("pendingInRoom: got (%+v, %v), want sess-1 pending", pending, ok)
	}

	paired, ok := s.pair("sess-1", "bob")
	if !ok {
		t.Fatal("pair: reported failure for an existing session")
	}
	if paired.Status != statusInProgress || paired.Player2ID != "bob" || paired.CurrentTurn != "alice" {
		t.Fatalf("pair: unexpected session %+v", paired)
	}

	if _, ok := s.pendingInRoom("ROOM01"); ok {
		t.Fatal("pendingInRoom: still reports a pending match after pairing")
	}
}

func TestConnectFourSessionForPlayerIsIdempotentWhileWaiting(t *testing.T) {
	s := newConnectFourStore()
	created := s.createWaiting("sess-1", "ROOM01", "alice")

	got, ok := s.sessionForPlayer("alice")
	if !ok || got.SessionID != created.SessionID {
		t.Fatalf("sessionForPlayer: got (%+v, %v), want the session alice just created", got, ok)
	}
}

func TestConnectFourRecordMoveClearsSessionByPlayerOnceFinished(t *testing.T) {
	s := newConnectFourStore()
	sess := s.createWaiting("sess-1", "ROOM01", "alice")
	s.pair(sess.SessionID, "bob")

	board := sess.Board // zero-value board is fine, contents don't matter here
	if _, ok := s.recordMove(sess.SessionID, board, "", statusWon, "alice"); !ok {
		t.Fatal("recordMove reported failure for an existing session")
	}

	if _, ok := s.sessionForPlayer("alice"); ok {
		t.Error("alice is still tracked as in a session after it finished")
	}
	if _, ok := s.sessionForPlayer("bob"); ok {
		t.Error("bob is still tracked as in a session after it finished")
	}
}

func TestConnectFourAbandonWhileWaitingDiscardsSilently(t *testing.T) {
	s := newConnectFourStore()
	sess := s.createWaiting("sess-1", "ROOM01", "alice")

	_, opponentID, hadSession := s.abandon("alice")
	if hadSession {
		t.Fatal("abandon reported a live opponent for a solo WAITING session")
	}
	if opponentID != "" {
		t.Fatalf("abandon returned opponentID %q, want empty", opponentID)
	}

	if _, ok := s.pendingInRoom("ROOM01"); ok {
		t.Error("room still shows a pending match after its only waiting player abandoned")
	}
	if _, ok := s.get(sess.SessionID); ok {
		t.Error("abandoned WAITING session was not removed from the store")
	}
}

func TestConnectFourAbandonMidMatchReportsOpponent(t *testing.T) {
	s := newConnectFourStore()
	sess := s.createWaiting("sess-1", "ROOM01", "alice")
	s.pair(sess.SessionID, "bob")

	left, opponentID, hadSession := s.abandon("alice")
	if !hadSession {
		t.Fatal("abandon reported no opponent for an IN_PROGRESS match")
	}
	if opponentID != "bob" {
		t.Fatalf("opponentID = %q, want %q", opponentID, "bob")
	}
	if left.Status != statusInProgress {
		t.Fatalf("abandon changed Status to %q on its own, want the caller to decide via recordMove", left.Status)
	}

	if _, ok := s.sessionForPlayer("alice"); ok {
		t.Error("alice is still tracked as in a session after abandoning it")
	}
	// bob is untracked immediately too, even though the session itself
	// (and its Status) isn't finalized until the caller's follow-up
	// recordMove call — see abandon's doc comment for why: it closes a
	// race where bob could otherwise re-trigger START_CONNECT_FOUR in
	// this gap and get paired into a session that then gets orphaned.
	if _, ok := s.sessionForPlayer("bob"); ok {
		t.Error("bob is still tracked as in a session immediately after his opponent abandoned it")
	}
}

func TestConnectFourAbandonAfterMatchAlreadyFinishedIsNoop(t *testing.T) {
	s := newConnectFourStore()
	sess := s.createWaiting("sess-1", "ROOM01", "alice")
	s.pair(sess.SessionID, "bob")
	s.recordMove(sess.SessionID, sess.Board, "", statusWon, "alice")

	_, _, hadSession := s.abandon("alice")
	if hadSession {
		t.Fatal("abandon reported a live opponent for a match that already finished")
	}
}
