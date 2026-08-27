// Package connectfour provides the board and win-detection logic for the
// two-player Connect Four mini-game. Unlike the word game and Connections
// puzzles, there's no puzzle data to load — the board starts empty and is
// shaped entirely by the two players' moves, so this package is smaller:
// just the rules, with all match/session state (whose turn, which two
// players, session id) living in the gateway layer instead.
package connectfour

import "fmt"

// Cols and Rows are the standard Connect Four board dimensions.
const (
	Cols = 7
	Rows = 6
)

// Cell is one board position: empty, or claimed by one of the two
// players.
type Cell byte

const (
	Empty     Cell = 0
	PlayerOne Cell = 1
	PlayerTwo Cell = 2
)

// Board is a fixed-size array (not a slice), so it copies by value —
// callers can hand out a Board without the caller/callee having to worry
// about one side mutating the other's copy.
type Board [Rows][Cols]Cell

// NewBoard returns an empty board.
func NewBoard() Board {
	return Board{}
}

// DropDisc returns a new board with player's disc dropped into col,
// settling on top of whatever's already stacked there (row 0 is the top,
// Rows-1 is the bottom), along with the row it landed on. board itself is
// never mutated. Returns an error if col is out of range or already
// full.
func DropDisc(board Board, col int, player Cell) (Board, int, error) {
	if col < 0 || col >= Cols {
		return board, -1, fmt.Errorf("connectfour: column %d out of range", col)
	}
	for row := Rows - 1; row >= 0; row-- {
		if board[row][col] == Empty {
			board[row][col] = player
			return board, row, nil
		}
	}
	return board, -1, fmt.Errorf("connectfour: column %d is full", col)
}

// IsFull reports whether the board has no empty cells left (a draw, if
// nobody has won). Checking just the top row is enough: gravity means a
// column's top cell is only ever empty if the column isn't full.
func IsFull(board Board) bool {
	for col := 0; col < Cols; col++ {
		if board[0][col] == Empty {
			return false
		}
	}
	return true
}

// directions to scan for four-in-a-row from any given cell: horizontal,
// vertical, and both diagonals. Each pairs with its own opposite when
// scanning from every cell, so this only needs to check one direction per
// axis, not both.
var directions = [4][2]int{{0, 1}, {1, 0}, {1, 1}, {1, -1}}

// CheckWin reports whether any player has four discs in a row —
// horizontally, vertically, or on either diagonal. The board is tiny
// (6x7), so a brute-force scan from every cell is simpler than tracking
// incremental state around the last move, and plenty fast.
func CheckWin(board Board) (winner Cell, won bool) {
	for row := 0; row < Rows; row++ {
		for col := 0; col < Cols; col++ {
			cell := board[row][col]
			if cell == Empty {
				continue
			}
			for _, d := range directions {
				if fourInARow(board, row, col, d[0], d[1], cell) {
					return cell, true
				}
			}
		}
	}
	return Empty, false
}

func fourInARow(board Board, row, col, dRow, dCol int, player Cell) bool {
	for i := 1; i < 4; i++ {
		r, c := row+dRow*i, col+dCol*i
		if r < 0 || r >= Rows || c < 0 || c >= Cols || board[r][c] != player {
			return false
		}
	}
	return true
}

// Reward amounts are flat rather than performance-scaled like the other
// two games' RewardForWin (fewer guesses/mistakes = more reward) — a
// Connect Four win doesn't have an equivalent "how well did you do"
// signal beyond winning itself.
const winReward = 30
const drawReward = 10

// RewardForWin returns the currency awarded to the match's winner.
func RewardForWin() int {
	return winReward
}

// RewardForDraw returns the currency awarded to EACH player when a match
// ends in a draw (the board fills with no winner).
func RewardForDraw() int {
	return drawReward
}
