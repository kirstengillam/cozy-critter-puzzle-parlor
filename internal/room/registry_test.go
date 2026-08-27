package room

import "testing"

func TestCreateAndExists(t *testing.T) {
	reg := NewRegistry()

	code, err := reg.Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(code) != codeLength {
		t.Fatalf("code %q has length %d, want %d", code, len(code), codeLength)
	}
	if !reg.Exists(code) {
		t.Fatalf("Exists(%q) = false, want true right after Create", code)
	}
}

func TestExistsUnknownCode(t *testing.T) {
	reg := NewRegistry()
	if reg.Exists("NOPE00") {
		t.Fatal("Exists returned true for a code that was never created")
	}
}

func TestDeleteReclaimsCode(t *testing.T) {
	reg := NewRegistry()

	code, err := reg.Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	reg.Delete(code)

	if reg.Exists(code) {
		t.Fatalf("Exists(%q) = true, want false after Delete", code)
	}
}

func TestCreateCodesAreUnique(t *testing.T) {
	reg := NewRegistry()
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		code, err := reg.Create()
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if seen[code] {
			t.Fatalf("Create produced duplicate code %q", code)
		}
		seen[code] = true
	}
}
