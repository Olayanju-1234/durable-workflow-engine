package cron

import (
	"testing"
	"time"
)

func mustTime(t *testing.T, s string, loc *time.Location) time.Time {
	t.Helper()
	v, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestNext(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no tzdata:", err)
	}
	for _, tt := range []struct {
		spec  string
		loc   *time.Location
		after string
		want  []string
	}{
		{"*/15 * * * *", nil, "2026-10-21 10:07", []string{"2026-10-21 10:15", "2026-10-21 10:30", "2026-10-21 10:45", "2026-10-21 11:00"}},
		{"0 9 * * MON-FRI", nil, "2026-10-23 09:00", []string{"2026-10-26 09:00", "2026-10-27 09:00"}}, // Fri → Mon
		{"30 2 1 * *", nil, "2026-01-15 00:00", []string{"2026-02-01 02:30", "2026-03-01 02:30"}},
		{"0 0 29 2 *", nil, "2026-03-01 00:00", []string{"2028-02-29 00:00"}},                                          // leap day
		{"0 12 13 * 5", nil, "2026-10-01 00:00", []string{"2026-10-02 12:00", "2026-10-09 12:00", "2026-10-13 12:00"}}, // dom OR dow
		{"5/20 0 * * *", nil, "2026-10-21 00:00", []string{"2026-10-21 00:05", "2026-10-21 00:25", "2026-10-21 00:45", "2026-10-22 00:05"}},
		{"0 0 * jan,jul sun", nil, "2026-06-30 00:00", []string{"2026-07-05 00:00"}},
		{"0 0 * * 7", nil, "2026-10-21 00:00", []string{"2026-10-25 00:00"}}, // 7 = Sunday
		{"@daily", nil, "2026-10-21 13:00", []string{"2026-10-22 00:00"}},
		{"@hourly", nil, "2026-10-21 13:00", []string{"2026-10-21 14:00"}},
		{"@weekly", nil, "2026-10-21 13:00", []string{"2026-10-25 00:00"}},
		{"@monthly", nil, "2026-12-15 00:00", []string{"2027-01-01 00:00"}},
		// Spring forward: 2:30 doesn't exist on 2026-03-08 in New York; the next
		// real 2:30 is the following day.
		{"30 2 * * *", ny, "2026-03-07 03:00", []string{"2026-03-09 02:30"}},
		// Fall back: 1:30 happens twice on 2026-11-01; it fires once that day.
		{"30 1 * * *", ny, "2026-10-31 02:00", []string{"2026-11-01 01:30", "2026-11-02 01:30"}},
	} {
		loc := tt.loc
		if loc == nil {
			loc = time.UTC
		}
		s, err := Parse(tt.spec, tt.loc)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tt.spec, err)
		}
		at := mustTime(t, tt.after, loc)
		for _, w := range tt.want {
			got := s.Next(at)
			if want := mustTime(t, w, loc); !got.Equal(want) {
				t.Fatalf("%q after %s: got %s, want %s", tt.spec, at, got.In(loc), want)
			}
			at = got
		}
	}
}

func TestEvery(t *testing.T) {
	s, err := Parse("@every 90s", nil)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1000, 0)
	got := s.Next(at)
	if got.Unix() != 1080 || s.Next(got).Unix() != 1170 {
		t.Fatalf("@every 90s after 1000: %d, %d; want 1080, 1170 (multiples of 90 since the epoch)", got.Unix(), s.Next(got).Unix())
	}
	if !s.Next(time.Unix(1080, 0)).Equal(time.Unix(1170, 0)) {
		t.Fatal("Next must be strictly after its argument")
	}
}

func TestParseErrors(t *testing.T) {
	for _, spec := range []string{
		"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *",
		"* * * * 8", "5-1 * * * *", "*/0 * * * *", "a * * * *", "@every 10ms", "@every soon", "@sometimes",
	} {
		if _, err := Parse(spec, nil); err == nil {
			t.Errorf("Parse(%q) succeeded, want an error", spec)
		}
	}
}

func TestImpossibleNeverFires(t *testing.T) {
	s, err := Parse("0 0 30 2 *", nil) // February 30th
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Next(time.Now()); !got.IsZero() {
		t.Fatalf("Feb 30 fired at %s", got)
	}
}
