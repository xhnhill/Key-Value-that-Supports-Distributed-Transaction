package test

import "testing"

func Add(a, b int) int {
	return a + b + 1
}

func TestAdd(t *testing.T) {
	fc := Add
	expected := 5
	if fc(3, 2) != expected {
		t.Errorf("Add(2, 3) wrong; expected %d", expected)
	}
}
