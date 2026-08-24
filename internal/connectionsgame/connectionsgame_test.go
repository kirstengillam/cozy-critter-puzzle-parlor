package connectionsgame

import "testing"

func TestRandomPuzzleHasFourGroupsOfFour(t *testing.T) {
	for i := 0; i < 20; i++ {
		p, err := RandomPuzzle()
		if err != nil {
			t.Fatalf("RandomPuzzle: %v", err)
		}
		if len(p.Answers) != GroupSize {
			t.Fatalf("puzzle %d has %d groups, want %d", p.ID, len(p.Answers), GroupSize)
		}
		for _, g := range p.Answers {
			if len(g.Members) != GroupSize {
				t.Fatalf("puzzle %d group %q has %d members, want %d", p.ID, g.Name, len(g.Members), GroupSize)
			}
		}
	}
}

func TestShuffledWordsHasAllSixteenUnique(t *testing.T) {
	p, err := RandomPuzzle()
	if err != nil {
		t.Fatalf("RandomPuzzle: %v", err)
	}
	words, err := ShuffledWords(p)
	if err != nil {
		t.Fatalf("ShuffledWords: %v", err)
	}
	if len(words) != GroupSize*GroupSize {
		t.Fatalf("got %d words, want %d", len(words), GroupSize*GroupSize)
	}
	seen := make(map[string]bool, len(words))
	for _, w := range words {
		if seen[w] {
			t.Fatalf("duplicate word %q in shuffled output", w)
		}
		seen[w] = true
	}
}

func testPuzzle() Puzzle {
	return Puzzle{
		ID:   1,
		Date: "2024-01-01",
		Answers: []Group{
			{Level: 0, Name: "WET WEATHER", Members: []string{"HAIL", "RAIN", "SLEET", "SNOW"}},
			{Level: 1, Name: "NBA TEAMS", Members: []string{"BUCKS", "HEAT", "JAZZ", "NETS"}},
			{Level: 2, Name: "KEYBOARD KEYS", Members: []string{"OPTION", "RETURN", "SHIFT", "TAB"}},
			{Level: 3, Name: "PALINDROMES", Members: []string{"KAYAK", "LEVEL", "MOM", "RACECAR"}},
		},
	}
}

func TestEvaluateGuessExactMatch(t *testing.T) {
	p := testPuzzle()
	idx, oneAway := EvaluateGuess([]string{"rain", "Hail", "SNOW", "sleet"}, p)
	if idx != 0 {
		t.Errorf("groupIndex = %d, want 0", idx)
	}
	if oneAway {
		t.Error("oneAway = true on an exact match, want false")
	}
}

func TestEvaluateGuessOneAway(t *testing.T) {
	p := testPuzzle()
	idx, oneAway := EvaluateGuess([]string{"HAIL", "RAIN", "SLEET", "TAB"}, p)
	if idx != -1 {
		t.Errorf("groupIndex = %d, want -1", idx)
	}
	if !oneAway {
		t.Error("oneAway = false on a 3/4 overlap, want true")
	}
}

func TestEvaluateGuessNoMatch(t *testing.T) {
	p := testPuzzle()
	idx, oneAway := EvaluateGuess([]string{"HAIL", "BUCKS", "OPTION", "MOM"}, p)
	if idx != -1 {
		t.Errorf("groupIndex = %d, want -1", idx)
	}
	if oneAway {
		t.Error("oneAway = true on a scattered guess, want false")
	}
}

func TestIsValidWord(t *testing.T) {
	p := testPuzzle()
	if !IsValidWord("hail", p) {
		t.Error(`IsValidWord("hail") = false, want true (case-insensitive)`)
	}
	if IsValidWord("zzzzz", p) {
		t.Error(`IsValidWord("zzzzz") = true, want false`)
	}
}

func TestRewardForWinDecreasesWithMoreMistakes(t *testing.T) {
	prev := RewardForWin(0)
	for m := 1; m < MaxMistakes; m++ {
		cur := RewardForWin(m)
		if cur >= prev {
			t.Fatalf("RewardForWin(%d) = %d is not less than RewardForWin(%d) = %d", m, cur, m-1, prev)
		}
		prev = cur
	}
}
