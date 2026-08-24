// Package connectionsgame provides the puzzle data and guess-evaluation
// logic for the NYT Connections-style grouping mini-game: sort 16 words
// into 4 hidden groups of 4. The puzzle data in data/puzzles.json is a
// vendored snapshot of the Eyefyre/NYT-Connections-Answers GitHub
// archive (https://github.com/Eyefyre/NYT-Connections-Answers) — see
// IMPLEMENTATION_PLAN.md for the data-source decision (no declared
// license, accepted for this non-monetized portfolio project).
package connectionsgame

import (
	"crypto/rand"
	_ "embed"
	"encoding/json"
	"math/big"
	"strings"
)

// GroupSize is the number of words per group, and the number of groups
// per puzzle (so 16 words total).
const GroupSize = 4

// MaxMistakes is how many wrong guesses end the game — matches the real
// game's 4-strikes limit (the 4th wrong guess loses).
const MaxMistakes = 4

// baseReward is the currency awarded for a win with zero mistakes; each
// mistake made along the way reduces it, floored at baseReward/GroupSize
// so a win always pays out something.
const baseReward = 40
const mistakePenalty = 8

// RewardForWin returns the currency reward for winning with mistakesUsed
// wrong guesses along the way — fewer mistakes means more reward.
func RewardForWin(mistakesUsed int) int {
	reward := baseReward - mistakesUsed*mistakePenalty
	if reward < mistakePenalty {
		reward = mistakePenalty
	}
	return reward
}

// Group is one of a puzzle's 4 categories. Level is the source archive's
// difficulty rank (0=yellow/easiest .. 3=purple/hardest), or -1 when the
// source didn't record one (NYT's API stopped exposing it — see the
// package doc comment's source link).
type Group struct {
	Level   int      `json:"level"`
	Name    string   `json:"group"`
	Members []string `json:"members"`
}

// Puzzle is one day's Connections puzzle: 4 groups of 4 words each.
type Puzzle struct {
	ID      int     `json:"id"`
	Date    string  `json:"date"`
	Answers []Group `json:"answers"`
}

//go:embed data/puzzles.json
var puzzlesRaw []byte

var puzzles []Puzzle

func init() {
	if err := json.Unmarshal(puzzlesRaw, &puzzles); err != nil {
		panic("connectionsgame: parse embedded puzzles.json: " + err.Error())
	}
}

// RandomPuzzle returns a random puzzle from the vendored archive.
func RandomPuzzle() (Puzzle, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(puzzles))))
	if err != nil {
		return Puzzle{}, err
	}
	return puzzles[n.Int64()], nil
}

// ShuffledWords returns all 16 of the puzzle's words in random order —
// what the client actually sees; group membership stays server-side.
func ShuffledWords(p Puzzle) ([]string, error) {
	words := make([]string, 0, len(p.Answers)*GroupSize)
	for _, g := range p.Answers {
		words = append(words, g.Members...)
	}
	for i := len(words) - 1; i > 0; i-- {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return nil, err
		}
		j := n.Int64()
		words[i], words[j] = words[j], words[i]
	}
	return words, nil
}

func normalize(s string) string {
	return strings.ToUpper(strings.TrimSpace(s))
}

// EvaluateGuess compares members (expected to be GroupSize words) against
// each of the puzzle's groups. groupIndex is the index into p.Answers of
// an exact match, or -1 if none matched exactly. oneAway reports whether
// the best-overlapping group (only meaningful when groupIndex is -1)
// shares exactly GroupSize-1 words with the guess, mirroring the real
// game's "one away" hint.
func EvaluateGuess(members []string, p Puzzle) (groupIndex int, oneAway bool) {
	guess := make(map[string]struct{}, len(members))
	for _, m := range members {
		guess[normalize(m)] = struct{}{}
	}

	bestOverlap, bestIndex := 0, -1
	for i, g := range p.Answers {
		overlap := 0
		for _, m := range g.Members {
			if _, ok := guess[normalize(m)]; ok {
				overlap++
			}
		}
		if overlap > bestOverlap {
			bestOverlap, bestIndex = overlap, i
		}
	}
	if bestOverlap == GroupSize {
		return bestIndex, false
	}
	return -1, bestOverlap == GroupSize-1
}

// IsValidWord reports whether word (case-insensitive) is one of the
// puzzle's 16 words — used to reject junk guesses before evaluating.
func IsValidWord(word string, p Puzzle) bool {
	target := normalize(word)
	for _, g := range p.Answers {
		for _, m := range g.Members {
			if normalize(m) == target {
				return true
			}
		}
	}
	return false
}
