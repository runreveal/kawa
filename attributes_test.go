package kawa

import (
	"fmt"
	"testing"
)

// ============================================================
// Adversarial / edge-case tests — TDD: written to break things
// ============================================================

// --- Zero-value safety ---

func TestZeroValueGetReturnsNotFound(t *testing.T) {
	var a Attributes
	v, ok := a.Get("anything")
	if ok || v != nil {
		t.Fatalf("Get on zero-value should return nil, false; got %v, %v", v, ok)
	}
}

func TestZeroValueDeleteIsNoop(t *testing.T) {
	var a Attributes
	b := a.Delete("nope")
	if b.Len() != 0 {
		t.Fatalf("Delete on zero-value should return empty; got Len=%d", b.Len())
	}
}

func TestZeroValueMergeWithZeroValue(t *testing.T) {
	var a, b Attributes
	c := a.Merge(b)
	if c.Len() != 0 {
		t.Fatalf("Merge of two zero-values should be empty; got Len=%d", c.Len())
	}
}

func TestZeroValueSetThenGet(t *testing.T) {
	var a Attributes
	b := a.Set("k", "v")
	got, ok := b.Get("k")
	if !ok || got != "v" {
		t.Fatalf("Set on zero-value then Get should work; got %v, %v", got, ok)
	}
	if a.Len() != 0 {
		t.Fatal("original zero-value must remain empty after Set")
	}
}

func TestZeroValueLen(t *testing.T) {
	var a Attributes
	if a.Len() != 0 {
		t.Fatalf("zero-value Len should be 0, got %d", a.Len())
	}
}

// --- Immutability guarantees ---

func TestSetDoesNotMutateOriginalSlice(t *testing.T) {
	a := Attributes{}.Set("k1", "v1").Set("k2", "v2")
	b := a.Set("k3", "v3")

	if a.Len() != 2 {
		t.Fatalf("original mutated after Set: Len=%d, want 2", a.Len())
	}
	if b.Len() != 3 {
		t.Fatalf("new attrs wrong Len: %d, want 3", b.Len())
	}
	if _, ok := a.Get("k3"); ok {
		t.Fatal("original must not contain k3 after Set on copy")
	}
}

func TestSetDoesNotMutateOriginalMap(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	if !a.isMap() {
		t.Fatal("should be map-backed")
	}
	original := a.Len()

	b := a.Set("extra", 999)
	if a.Len() != original {
		t.Fatalf("original map mutated: Len=%d, want %d", a.Len(), original)
	}
	if _, ok := a.Get("extra"); ok {
		t.Fatal("original must not contain extra after Set on copy")
	}
	if b.Len() != original+1 {
		t.Fatalf("new attrs wrong Len: %d, want %d", b.Len(), original+1)
	}
}

func TestDeleteDoesNotMutateOriginalSlice(t *testing.T) {
	a := Attributes{}.Set("k1", 1).Set("k2", 2)
	b := a.Delete("k1")

	if a.Len() != 2 {
		t.Fatalf("original mutated after Delete: Len=%d", a.Len())
	}
	if b.Len() != 1 {
		t.Fatalf("deleted attrs wrong Len: %d", b.Len())
	}
}

func TestDeleteDoesNotMutateOriginalMap(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	original := a.Len()
	b := a.Delete("k0")

	if a.Len() != original {
		t.Fatalf("original map mutated after Delete: Len=%d", a.Len())
	}
	if _, ok := a.Get("k0"); !ok {
		t.Fatal("original must still contain k0")
	}
	if _, ok := b.Get("k0"); ok {
		t.Fatal("deleted copy must not contain k0")
	}
}

func TestMergeDoesNotMutateEitherSide(t *testing.T) {
	a := Attributes{}.Set("a", 1).Set("shared", "fromA")
	b := Attributes{}.Set("b", 2).Set("shared", "fromB")

	c := a.Merge(b)

	// a unchanged
	va, _ := a.Get("shared")
	if va != "fromA" {
		t.Fatalf("a mutated: shared=%v", va)
	}
	if a.Len() != 2 {
		t.Fatalf("a Len mutated: %d", a.Len())
	}

	// b unchanged
	vb, _ := b.Get("shared")
	if vb != "fromB" {
		t.Fatalf("b mutated: shared=%v", vb)
	}

	// c correct
	vc, _ := c.Get("shared")
	if vc != "fromB" {
		t.Fatalf("merge precedence wrong: shared=%v, want fromB", vc)
	}
	if c.Len() != 3 {
		t.Fatalf("merged Len wrong: %d, want 3", c.Len())
	}
}

// --- Nil and empty-string edge cases ---

func TestEmptyStringKey(t *testing.T) {
	a := Attributes{}.Set("", "empty-key-value")
	got, ok := a.Get("")
	if !ok || got != "empty-key-value" {
		t.Fatalf("empty string key should work; got %v, %v", got, ok)
	}
}

func TestNilValue(t *testing.T) {
	a := Attributes{}.Set("k", nil)
	got, ok := a.Get("k")
	if !ok {
		t.Fatal("key with nil value should be found")
	}
	if got != nil {
		t.Fatalf("expected nil value, got %v", got)
	}
}

func TestSetOverwriteWithNil(t *testing.T) {
	a := Attributes{}.Set("k", "real")
	b := a.Set("k", nil)
	got, ok := b.Get("k")
	if !ok {
		t.Fatal("key should still exist after overwrite with nil")
	}
	if got != nil {
		t.Fatalf("expected nil after overwrite, got %v", got)
	}
}

// --- Delete edge cases ---

func TestDeleteNonExistentKeySlice(t *testing.T) {
	a := Attributes{}.Set("k1", 1)
	b := a.Delete("doesnotexist")
	if b.Len() != 1 {
		t.Fatalf("delete of non-existent key should be noop; Len=%d", b.Len())
	}
}

func TestDeleteNonExistentKeyMap(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	original := a.Len()
	b := a.Delete("doesnotexist")
	if b.Len() != original {
		t.Fatalf("delete of non-existent key in map should be noop; Len=%d", b.Len())
	}
}

func TestDoubleDelete(t *testing.T) {
	a := Attributes{}.Set("k1", 1).Set("k2", 2)
	b := a.Delete("k1").Delete("k1")
	if b.Len() != 1 {
		t.Fatalf("double delete should leave 1 entry; got %d", b.Len())
	}
}

func TestDeleteAllEntries(t *testing.T) {
	a := Attributes{}.Set("k1", 1).Set("k2", 2)
	b := a.Delete("k1").Delete("k2")
	if b.Len() != 0 {
		t.Fatalf("deleting all entries should yield Len=0; got %d", b.Len())
	}
	if _, ok := b.Get("k1"); ok {
		t.Fatal("k1 should not be found")
	}
}

func TestDeleteThenSetSameKey(t *testing.T) {
	a := Attributes{}.Set("k", "first")
	b := a.Delete("k").Set("k", "second")
	got, ok := b.Get("k")
	if !ok || got != "second" {
		t.Fatalf("set after delete should work; got %v, %v", got, ok)
	}
	if b.Len() != 1 {
		t.Fatalf("should have 1 entry; got %d", b.Len())
	}
}

// --- Promotion / demotion boundary ---

func TestExactlyAtThresholdStaysSlice(t *testing.T) {
	a := Attributes{}
	for i := 0; i < attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	if a.isMap() {
		t.Fatal("exactly attrThreshold entries should stay as slice")
	}
	if a.Len() != attrThreshold {
		t.Fatalf("Len should be %d, got %d", attrThreshold, a.Len())
	}
}

func TestPromotionPreservesAllData(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	if !a.isMap() {
		t.Fatal("should be map after crossing threshold")
	}
	for i := 0; i <= attrThreshold; i++ {
		got, ok := a.Get(fmt.Sprintf("k%d", i))
		if !ok || got != i {
			t.Fatalf("data lost during promotion: k%d=%v, ok=%v", i, got, ok)
		}
	}
}

func TestDemotionPreservesAllData(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	b := a.Delete("k0")
	if b.isMap() {
		t.Fatal("should demote to slice after delete to threshold")
	}
	for i := 1; i <= attrThreshold; i++ {
		got, ok := b.Get(fmt.Sprintf("k%d", i))
		if !ok || got != i {
			t.Fatalf("data lost during demotion: k%d=%v, ok=%v", i, got, ok)
		}
	}
}

func TestPromoteThenUpdateExistingStaysMap(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	b := a.Set("k0", 999) // update existing in map mode
	if !b.isMap() {
		t.Fatal("updating existing key in map should stay map")
	}
	got, _ := b.Get("k0")
	if got != 999 {
		t.Fatalf("expected updated value 999, got %v", got)
	}
	if b.Len() != a.Len() {
		t.Fatal("update should not change Len")
	}
}

// --- Merge edge cases ---

func TestMergeBothMapBacked(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("a%d", i), i)
	}
	b := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		b = b.Set(fmt.Sprintf("b%d", i), i)
	}
	c := a.Merge(b)
	expected := (attrThreshold + 1) * 2
	if c.Len() != expected {
		t.Fatalf("merge of two map-backed should have %d entries; got %d", expected, c.Len())
	}
	if !c.isMap() {
		t.Fatal("merged result should be map-backed")
	}
}

func TestMergeSliceIntoMapBacked(t *testing.T) {
	// left is map-backed, right is slice-backed
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("a%d", i), i)
	}
	b := Attributes{}.Set("b1", 1).Set("a0", "override")

	c := a.Merge(b)
	got, _ := c.Get("a0")
	if got != "override" {
		t.Fatalf("right side should override left; got %v", got)
	}
	gotB, ok := c.Get("b1")
	if !ok || gotB != 1 {
		t.Fatalf("right-only key missing; got %v, %v", gotB, ok)
	}
}

func TestMergeMapBackedIntoSlice(t *testing.T) {
	// left is slice-backed, right is map-backed
	a := Attributes{}.Set("a1", 1)
	b := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		b = b.Set(fmt.Sprintf("b%d", i), i)
	}
	c := a.Merge(b)
	gotA, ok := c.Get("a1")
	if !ok || gotA != 1 {
		t.Fatalf("left-only key missing; got %v, %v", gotA, ok)
	}
	gotB, ok := c.Get("b0")
	if !ok || gotB != 0 {
		t.Fatalf("right key missing; got %v, %v", gotB, ok)
	}
}

func TestMergeWithSelf(t *testing.T) {
	a := Attributes{}.Set("k", "v")
	b := a.Merge(a)
	if b.Len() != 1 {
		t.Fatalf("self-merge should keep same count; got %d", b.Len())
	}
	got, _ := b.Get("k")
	if got != "v" {
		t.Fatalf("self-merge changed value; got %v", got)
	}
}

func TestMergeAllOverlapping(t *testing.T) {
	a := Attributes{}.Set("k1", "a1").Set("k2", "a2")
	b := Attributes{}.Set("k1", "b1").Set("k2", "b2")
	c := a.Merge(b)
	if c.Len() != 2 {
		t.Fatalf("full-overlap merge should have 2 entries; got %d", c.Len())
	}
	g1, _ := c.Get("k1")
	g2, _ := c.Get("k2")
	if g1 != "b1" || g2 != "b2" {
		t.Fatalf("right side should win; got k1=%v, k2=%v", g1, g2)
	}
}

func TestMergeEmptyIntoPopulated(t *testing.T) {
	a := Attributes{}.Set("k", 1)
	b := Attributes{}
	c := a.Merge(b)
	if c.Len() != 1 {
		t.Fatalf("merge with empty should not change Len; got %d", c.Len())
	}
}

func TestMergePopulatedIntoEmpty(t *testing.T) {
	a := Attributes{}
	b := Attributes{}.Set("k", 1)
	c := a.Merge(b)
	if c.Len() != 1 {
		t.Fatalf("merge empty with populated should work; got %d", c.Len())
	}
}

// --- GetAs edge cases ---

func TestGetAsWrongType(t *testing.T) {
	a := Attributes{}.Set("k", 42)
	got, ok := GetAs[string](a, "k")
	if ok {
		t.Fatalf("GetAs with wrong type should return false; got %v", got)
	}
	if got != "" {
		t.Fatalf("GetAs with wrong type should return zero value; got %v", got)
	}
}

func TestGetAsMissingKey(t *testing.T) {
	a := Attributes{}.Set("k", 42)
	got, ok := GetAs[int](a, "missing")
	if ok {
		t.Fatal("GetAs for missing key should return false")
	}
	if got != 0 {
		t.Fatalf("GetAs for missing key should return zero; got %v", got)
	}
}

func TestGetAsNilValue(t *testing.T) {
	a := Attributes{}.Set("k", nil)
	got, ok := GetAs[string](a, "k")
	if ok {
		t.Fatal("GetAs with nil value and string type should return false")
	}
	if got != "" {
		t.Fatalf("should be zero value; got %v", got)
	}
}

func TestGetAsCorrectType(t *testing.T) {
	a := Attributes{}.Set("k", 42)
	got, ok := GetAs[int](a, "k")
	if !ok || got != 42 {
		t.Fatalf("GetAs correct type should work; got %v, %v", got, ok)
	}
}

// --- NewAttributes edge cases ---

func TestNewAttributesZeroSize(t *testing.T) {
	a := NewAttributes(0)
	if a.Len() != 0 || a.isMap() {
		t.Fatal("NewAttributes(0) should be empty slice-backed")
	}
}

func TestNewAttributesNegativeSize(t *testing.T) {
	a := NewAttributes(-100)
	if a.Len() != 0 || a.isMap() {
		t.Fatal("NewAttributes(-100) should be empty slice-backed")
	}
}

func TestNewAttributesLargePrealloc(t *testing.T) {
	a := NewAttributes(100)
	if !a.isMap() {
		t.Fatal("NewAttributes(100) should be map-backed")
	}
	if a.Len() != 0 {
		t.Fatalf("pre-allocated should have Len=0; got %d", a.Len())
	}
}

func TestNewAttributesNoArgs(t *testing.T) {
	a := NewAttributes()
	if a.Len() != 0 || a.isMap() {
		t.Fatal("NewAttributes() should be empty slice-backed")
	}
}

// --- rangeFunc completeness ---

func TestRangeFuncVisitsAllSlice(t *testing.T) {
	a := Attributes{}.Set("a", 1).Set("b", 2).Set("c", 3)
	seen := map[string]any{}
	a.rangeFunc(func(k string, v any) { seen[k] = v })
	if len(seen) != 3 {
		t.Fatalf("rangeFunc should visit 3 entries; visited %d", len(seen))
	}
}

func TestRangeFuncVisitsAllMap(t *testing.T) {
	a := Attributes{}
	for i := 0; i <= attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	count := 0
	a.rangeFunc(func(k string, v any) { count++ })
	if count != attrThreshold+1 {
		t.Fatalf("rangeFunc should visit %d entries; visited %d", attrThreshold+1, count)
	}
}

// --- Thrash: rapid promote/demote cycling ---

func TestPromoteDemoteCycle(t *testing.T) {
	a := Attributes{}
	// Build up to threshold
	for i := 0; i < attrThreshold; i++ {
		a = a.Set(fmt.Sprintf("k%d", i), i)
	}
	// Promote
	a = a.Set("overflow", 999)
	if !a.isMap() {
		t.Fatal("should be map after promote")
	}
	// Demote
	a = a.Delete("overflow")
	if a.isMap() {
		t.Fatal("should be slice after demote")
	}
	// Promote again
	a = a.Set("overflow2", 888)
	if !a.isMap() {
		t.Fatal("should be map after re-promote")
	}
	// All original keys present
	for i := 0; i < attrThreshold; i++ {
		got, ok := a.Get(fmt.Sprintf("k%d", i))
		if !ok || got != i {
			t.Fatalf("data lost after promote/demote cycle: k%d=%v, ok=%v", i, got, ok)
		}
	}
	got, ok := a.Get("overflow2")
	if !ok || got != 888 {
		t.Fatalf("new key missing after cycle; got %v, %v", got, ok)
	}
}

// --- Map-backed Attributes created via NewAttributes(large) with few entries ---

func TestMapBackedWithFewEntriesMerge(t *testing.T) {
	// NewAttributes(20) creates a map-backed Attributes, but we only add 1 key.
	// Merge with a slice-backed should still work correctly.
	a := NewAttributes(20).Set("a", 1)
	b := Attributes{}.Set("b", 2)

	if !a.isMap() {
		t.Fatal("a should be map-backed from NewAttributes(20)")
	}
	if b.isMap() {
		t.Fatal("b should be slice-backed")
	}

	c := a.Merge(b)
	if c.Len() != 2 {
		t.Fatalf("expected 2 entries; got %d", c.Len())
	}

	gotA, ok := c.Get("a")
	if !ok || gotA != 1 {
		t.Fatalf("key a missing: %v, %v", gotA, ok)
	}
	gotB, ok := c.Get("b")
	if !ok || gotB != 2 {
		t.Fatalf("key b missing: %v, %v", gotB, ok)
	}
}

func TestMapBackedWithFewEntriesDelete(t *testing.T) {
	// Map-backed with 1 entry — delete should demote to empty slice.
	a := NewAttributes(20).Set("k", 1)
	b := a.Delete("k")
	if b.Len() != 0 {
		t.Fatalf("should be empty after delete; got %d", b.Len())
	}
	if b.isMap() {
		t.Fatal("should demote to slice after delete leaves 0 entries")
	}
}

func TestMapBackedSetExistingDoesNotGrow(t *testing.T) {
	a := NewAttributes(20).Set("k", 1)
	b := a.Set("k", 2)
	if b.Len() != 1 {
		t.Fatalf("overwrite should not grow; Len=%d", b.Len())
	}
	got, _ := b.Get("k")
	if got != 2 {
		t.Fatalf("overwrite failed; got %v", got)
	}
}