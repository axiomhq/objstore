package rangeread

import (
	"math"
	"reflect"
	"slices"
	"testing"
)

func TestRangePlanSpendsExtraOnCheaperGaps(t *testing.T) {
	// Object-order greedy spends all 256 KiB on eight 32 KiB gaps,
	// yielding 66 reads. The same allowance pays for 64 later 4 KiB gaps:
	// nine unmerged expensive spans plus one cheap group = ten reads.
	var spans []Extent
	for i := range 9 {
		spans = append(spans, Extent{"a", int64(i) * (8 + 32<<10), 8})
	}
	for i := range 65 {
		spans = append(spans, Extent{"b", int64(i) * (8 + 4<<10), 8})
	}
	got, err := Plan(spans, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("74 spans require %d reads, want 10", len(got))
	}
	var extra, transferred int64
	for _, r := range got {
		extra += r.Extra
		transferred += r.Length
	}
	if extra != 256<<10 || transferred != 74*8+extra {
		t.Fatalf("extra/transferred = %d/%d; semantic bytes must remain 592", extra, transferred)
	}
	for _, span := range spans {
		covered := false
		for _, r := range got {
			if span.Object == r.Object && span.Offset >= r.Offset && span.Offset+span.Length <= r.Offset+r.Length {
				covered = true
			}
		}
		if !covered {
			t.Fatalf("lost requested span %+v", span)
		}
	}
	slices.Reverse(spans)
	reversed, err := Plan(spans, Config{})
	if err != nil || !reflect.DeepEqual(got, reversed) {
		t.Fatalf("input order changed deterministic plan: %v, %v", reversed, err)
	}
}

func TestRangePlanCheaperMiddleCannotIncreaseRequests(t *testing.T) {
	// Taking the middle gap first makes a five-byte group with no room
	// for either neighbor (three requests). Greedy makes two six-byte
	// groups; retain that better plan under the same range limit.
	spans := []Extent{{"a", 0, 2}, {"a", 4, 2}, {"a", 7, 2}, {"a", 11, 2}}
	cfg := Config{MaxGapBytes: 2, MaxExtraBytes: 10, MaxRangeBytes: 6, MaxInFlightBytes: 12}
	got, err := Plan(spans, cfg)
	want := []Planned{{Extent{"a", 0, 6}, 2}, {Extent{"a", 7, 6}, 2}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("range-limit fallback = %v, %v; want %v", got, err, want)
	}
}

func TestRangePlanCheapGapsPreserveSplitsAndLargeOffsets(t *testing.T) {
	cfg := Config{MaxGapBytes: 2, MaxExtraBytes: 3, MaxRangeBytes: 6, MaxInFlightBytes: 12}
	spans := []Extent{
		{"a", 0, 13}, {"a", 2, 3}, // overlap union, then two full pieces and a tail
		{"a", 15, 1}, // expensive gap consumes greedy's allowance
		{"b", math.MaxInt64 - 8, 1}, {"b", math.MaxInt64 - 6, 1},
		{"b", math.MaxInt64 - 4, 1}, {"b", math.MaxInt64 - 2, 1},
	}
	got, err := Plan(spans, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var extra int64
	for _, r := range got {
		if r.Length > cfg.MaxRangeBytes || r.Offset > math.MaxInt64-r.Length {
			t.Fatalf("invalid bounded range %+v", r)
		}
		extra += r.Extra
	}
	if extra > cfg.MaxExtraBytes || len(got) > 6 {
		t.Fatalf("plan exceeds greedy request or byte bounds: %v", got)
	}
	// The first twelve bytes are required and remain split, regardless of
	// which later gap is paid. No overlap is counted as extra transfer.
	if got[0] != (Planned{Extent{"a", 0, 6}, 0}) || got[1] != (Planned{Extent{"a", 6, 6}, 0}) {
		t.Fatalf("changed required splits: %v", got)
	}
}

func TestRangePlanBudgetsAndIdentity(t *testing.T) {
	cfg := Config{MaxGapBytes: 4, MaxExtraBytes: 5, MaxRangeBytes: 32, MaxInFlightBytes: 64, Concurrency: 2}
	in := []Extent{{"b", 0, 4}, {"a", 13, 2}, {"a", 0, 4}, {"a", 2, 4}, {"a", 0, 4}, {"a", 9, 2}}
	original := append([]Extent(nil), in...)
	got, err := Plan(in, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []Planned{{Extent{"a", 0, 15}, 5}, {Extent{"b", 0, 4}, 0}}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(in, original) {
		t.Fatalf("plan = %v, want %v; input mutated = %v", got, want, !reflect.DeepEqual(in, original))
	}
}

func TestRangePlanSplitsAndWaveBudget(t *testing.T) {
	cfg := Config{MaxGapBytes: 3, MaxExtraBytes: 3, MaxRangeBytes: 8, MaxInFlightBytes: 16, Concurrency: 1}
	got, err := Plan([]Extent{{"a", 0, 18}, {"a", 20, 2}, {"b", 0, 2}, {"b", 4, 2}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []Planned{{Extent{"a", 0, 8}, 0}, {Extent{"a", 8, 8}, 0}, {Extent{"a", 16, 6}, 2}, {Extent{"b", 0, 2}, 0}, {Extent{"b", 4, 2}, 0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
}

func TestRangePlanRejectsInvalidInput(t *testing.T) {
	for _, r := range []Extent{{"", 0, 1}, {"a#extent", 0, 1}, {"a", -1, 1}, {"a", 0, 0}, {"a", 0, -1}, {"a", math.MaxInt64, 1}} {
		if _, err := Plan([]Extent{r}, Config{}); err == nil {
			t.Fatalf("accepted invalid extent %+v", r)
		}
	}
	for _, cfg := range []Config{{MaxGapBytes: -1}, {MaxExtraBytes: -1}, {MaxRangeBytes: -1}, {MaxInFlightBytes: -1}, {Concurrency: -1}, {Concurrency: 33}, {MaxRangeBytes: 20, MaxInFlightBytes: 10}} {
		if _, err := Plan(nil, cfg); err == nil {
			t.Fatalf("accepted invalid config %+v", cfg)
		}
	}
	if _, err := Plan([]Extent{{"a", math.MaxInt64 - 1, 1}}, Config{}); err != nil {
		t.Fatal(err)
	}
}
