package connectfour

import "testing"

func TestDropDiscStacksOnTopOfExistingDiscs(t *testing.T) {
	board := NewBoard()

	board, row, err := DropDisc(board, 3, PlayerOne)
	if err != nil {
		t.Fatalf("DropDisc: %v", err)
	}
	if row != Rows-1 {
		t.Fatalf("first disc in an empty column landed at row %d, want %d (the bottom)", row, Rows-1)
	}

	board, row, err = DropDisc(board, 3, PlayerTwo)
	if err != nil {
		t.Fatalf("DropDisc: %v", err)
	}
	if row != Rows-2 {
		t.Fatalf("second disc landed at row %d, want %d (stacked on the first)", row, Rows-2)
	}
	if board[Rows-1][3] != PlayerOne || board[Rows-2][3] != PlayerTwo {
		t.Fatalf("unexpected column contents: %v", board)
	}
}

func TestDropDiscDoesNotMutateInputBoard(t *testing.T) {
	before := NewBoard()
	after, _, err := DropDisc(before, 0, PlayerOne)
	if err != nil {
		t.Fatalf("DropDisc: %v", err)
	}
	if before[Rows-1][0] != Empty {
		t.Fatal("DropDisc mutated its input board in place")
	}
	if after[Rows-1][0] != PlayerOne {
		t.Fatal("DropDisc's returned board is missing the new disc")
	}
}

func TestDropDiscRejectsOutOfRangeColumn(t *testing.T) {
	board := NewBoard()
	if _, _, err := DropDisc(board, -1, PlayerOne); err == nil {
		t.Error("DropDisc(-1) succeeded, want an error")
	}
	if _, _, err := DropDisc(board, Cols, PlayerOne); err == nil {
		t.Errorf("DropDisc(%d) succeeded, want an error", Cols)
	}
}

func TestDropDiscRejectsFullColumn(t *testing.T) {
	board := NewBoard()
	var err error
	for i := 0; i < Rows; i++ {
		board, _, err = DropDisc(board, 0, PlayerOne)
		if err != nil {
			t.Fatalf("DropDisc: %v", err)
		}
	}
	if _, _, err := DropDisc(board, 0, PlayerTwo); err == nil {
		t.Error("DropDisc into a full column succeeded, want an error")
	}
}

func TestIsFull(t *testing.T) {
	board := NewBoard()
	if IsFull(board) {
		t.Fatal("IsFull(empty board) = true, want false")
	}

	var err error
	for col := 0; col < Cols; col++ {
		for row := 0; row < Rows; row++ {
			board, _, err = DropDisc(board, col, PlayerOne)
			if err != nil {
				t.Fatalf("DropDisc: %v", err)
			}
		}
	}
	if !IsFull(board) {
		t.Fatal("IsFull(completely filled board) = false, want true")
	}
}

func TestCheckWinNoWinner(t *testing.T) {
	board := NewBoard()
	if _, won := CheckWin(board); won {
		t.Fatal("CheckWin(empty board) reported a winner")
	}
}

func TestCheckWinHorizontal(t *testing.T) {
	board := NewBoard()
	for col := 0; col < 4; col++ {
		board[Rows-1][col] = PlayerOne
	}
	winner, won := CheckWin(board)
	if !won || winner != PlayerOne {
		t.Fatalf("CheckWin = (%v, %v), want (PlayerOne, true)", winner, won)
	}
}

func TestCheckWinVertical(t *testing.T) {
	board := NewBoard()
	for row := Rows - 1; row >= Rows-4; row-- {
		board[row][2] = PlayerTwo
	}
	winner, won := CheckWin(board)
	if !won || winner != PlayerTwo {
		t.Fatalf("CheckWin = (%v, %v), want (PlayerTwo, true)", winner, won)
	}
}

func TestCheckWinDiagonalDownRight(t *testing.T) {
	board := NewBoard()
	for i := 0; i < 4; i++ {
		board[i][i] = PlayerOne
	}
	winner, won := CheckWin(board)
	if !won || winner != PlayerOne {
		t.Fatalf("CheckWin = (%v, %v), want (PlayerOne, true)", winner, won)
	}
}

func TestCheckWinDiagonalDownLeft(t *testing.T) {
	board := NewBoard()
	for i := 0; i < 4; i++ {
		board[i][Cols-1-i] = PlayerTwo
	}
	winner, won := CheckWin(board)
	if !won || winner != PlayerTwo {
		t.Fatalf("CheckWin = (%v, %v), want (PlayerTwo, true)", winner, won)
	}
}

func TestCheckWinThreeInARowIsNotAWin(t *testing.T) {
	board := NewBoard()
	for col := 0; col < 3; col++ {
		board[Rows-1][col] = PlayerOne
	}
	if _, won := CheckWin(board); won {
		t.Fatal("CheckWin reported a win on only three in a row")
	}
}

func TestRewardAmountsArePositiveAndWinBeatsDraw(t *testing.T) {
	if RewardForWin() <= RewardForDraw() {
		t.Fatalf("RewardForWin() = %d should be greater than RewardForDraw() = %d", RewardForWin(), RewardForDraw())
	}
	if RewardForDraw() <= 0 {
		t.Fatalf("RewardForDraw() = %d, want > 0", RewardForDraw())
	}
}
