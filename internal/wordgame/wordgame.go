// Package wordgame provides the word list and guess-evaluation logic
// for the Wordle-style mini-game (Milestone 3). The word lists in data/
// are the widely-mirrored original NYT Wordle answer/allowed-guess
// lists — long since extracted and redistributed across the
// open-source Wordle-clone ecosystem; see data/answers.txt and
// data/allowed_guesses.txt.
package wordgame

import (
	"crypto/rand"
	_ "embed"
	"math/big"
	"strings"
)

const WordLength = 5

// MaxGuesses is the standard Wordle attempt limit.
const MaxGuesses = 6

// baseReward is the currency awarded for a win taking the full
// MaxGuesses attempts; each guess saved below that adds one more
// baseReward's worth.
const baseReward = 10

// RewardForWin returns the currency reward for winning in guessesUsed
// attempts (1..MaxGuesses) — fewer guesses means more reward.
func RewardForWin(guessesUsed int) int {
	remaining := MaxGuesses - guessesUsed + 1
	if remaining < 1 {
		remaining = 1
	}
	return remaining * baseReward
}

//go:embed data/answers.txt
var answersRaw string

//go:embed data/allowed_guesses.txt
var allowedGuessesRaw string

var (
	answers        []string
	allowedGuesses map[string]struct{}
)

func init() {
	answers = splitLines(answersRaw)
	allowedGuesses = make(map[string]struct{}, len(answers)+13000)
	for _, w := range splitLines(allowedGuessesRaw) {
		allowedGuesses[w] = struct{}{}
	}
}

func splitLines(raw string) []string {
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// RandomAnswer returns a random target word from the answers list.
func RandomAnswer() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(answers))))
	if err != nil {
		return "", err
	}
	return answers[n.Int64()], nil
}

// IsValidGuess reports whether word (case-insensitive) is in the
// allowed-guesses list — the broader list, not just answers, so a
// legitimate word is never rejected just for not being a possible
// answer.
func IsValidGuess(word string) bool {
	_, ok := allowedGuesses[strings.ToLower(word)]
	return ok
}

// LetterState is the per-letter feedback for a guess, Wordle-style.
type LetterState string

const (
	Correct LetterState = "CORRECT"
	Present LetterState = "PRESENT"
	Absent  LetterState = "ABSENT"
)

// Evaluate computes per-letter feedback for guess against target.
// guess and target must both be WordLength letters. Follows standard
// Wordle duplicate-letter rules: a repeated letter is only marked
// PRESENT as many times as it remains unaccounted for by CORRECT
// matches (and earlier PRESENT matches in the same guess).
func Evaluate(guess, target string) []LetterState {
	guess = strings.ToLower(guess)
	target = strings.ToLower(target)

	result := make([]LetterState, len(guess))
	remaining := make(map[byte]int)

	for i := 0; i < len(guess); i++ {
		if guess[i] == target[i] {
			result[i] = Correct
		} else {
			remaining[target[i]]++
		}
	}
	for i := 0; i < len(guess); i++ {
		if result[i] == Correct {
			continue
		}
		if remaining[guess[i]] > 0 {
			result[i] = Present
			remaining[guess[i]]--
		} else {
			result[i] = Absent
		}
	}
	return result
}
