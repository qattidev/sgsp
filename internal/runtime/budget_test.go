package runtime

import "testing"

func TestGlobalBudget(t *testing.T) {
	budget := NewBudget(5)
	if !budget.Acquire(3) || budget.Acquire(3) || budget.Used() != 3 {
		t.Fatal("budget admission failed")
	}
	budget.Release(3)
	if !budget.Acquire(5) || budget.Used() != 5 {
		t.Fatal("released budget was not reusable")
	}
}
