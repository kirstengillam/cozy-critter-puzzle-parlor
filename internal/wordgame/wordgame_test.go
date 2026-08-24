package wordgame

import "testing"

func TestRandomAnswerIsFiveLetters(t *testing.T) {
	for i := 0; i < 20; i++ {
		w, err := RandomAnswer()
		if err != nil {
			t.Fatalf("RandomAnswer: %v", err)
		}
		if len(w) != WordLength {
			t.Fatalf("RandomAnswer() = %q, want length %d", w, WordLength)
		}
	}
}

func TestIsValidGuess(t *testing.T) {
	if !IsValidGuess("crane") {
		t.Error(`IsValidGuess("crane") = false, want true`)
	}
	if !IsValidGuess("CRANE") {
		t.Error(`IsValidGuess("CRANE") = false, want true (case-insensitive)`)
	}
	if IsValidGuess("zzzzz") {
		t.Error(`IsValidGuess("zzzzz") = true, want false`)
	}
}

func TestEvaluateAllCorrect(t *testing.T) {
	got := Evaluate("crane", "crane")
	for i, s := range got {
		if s != Correct {
			t.Fatalf("index %d: got %s, want %s", i, s, Correct)
		}
	}
}

func TestEvaluateAllAbsent(t *testing.T) {
	got := Evaluate("might", "usual")
	for i, s := range got {
		if s != Absent {
			t.Fatalf("index %d: got %s, want %s", i, s, Absent)
		}
	}
}

func TestEvaluateDuplicateLetters(t *testing.T) {
	// target "abbey" = a,b,b,e,y. Guessing "bobby" = b,o,b,b,y:
	// idx2 b==b and idx4 y==y are exact matches (CORRECT), leaving
	// target's idx0 'a', idx1 'b', idx3 'e' unmatched (one 'b' left over).
	// Of guess's two non-matching b's (idx0, idx3), only the first one
	// scanned can claim that single remaining 'b' -> PRESENT; the second
	// finds none left -> ABSENT. 'o' isn't in the target at all -> ABSENT.
	got := Evaluate("bobby", "abbey")
	want := []LetterState{Present, Absent, Correct, Absent, Correct}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("index %d: got %s, want %s (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestEvaluateOveraccountingDoesNotDoubleCountPresent(t *testing.T) {
	// target "melon" has one 'l'. Guessing "lilly" (four l's): only the
	// single target 'l' should ever be claimed as PRESENT/CORRECT once;
	// the rest must be ABSENT.
	got := Evaluate("lilly", "melon")
	lCount := 0
	for i, s := range got {
		if got[i] == Correct || got[i] == Present {
			lCount++
		}
		_ = i
		_ = s
	}
	if lCount != 1 {
		t.Fatalf("got %v, want exactly one CORRECT/PRESENT among the l's", got)
	}
}
