// Package cron parses cron expressions and computes when they next fire. It is
// hand-written to keep the engine free of dependencies.
//
// Supported: the five standard fields (minute hour day-of-month month
// day-of-week) with *, numbers, ranges (1-5), steps (*/15, 0-30/10, 5/10),
// lists (1,15,30), month and weekday names (JAN-DEC, SUN-SAT; 7 is also
// Sunday); the macros @yearly (@annually), @monthly, @weekly, @daily
// (@midnight) and @hourly; and @every <duration> (at least 1s), which fires at
// multiples of the duration since the Unix epoch. As in Vixie cron, when both
// day fields are restricted a day matches if either does.
package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule says when something next happens.
type Schedule interface {
	// Next returns the first fire time strictly after t, or the zero time if
	// there is none within five years.
	Next(t time.Time) time.Time
}

// Parse reads a cron expression whose times are wall-clock times in loc
// (UTC when loc is nil).
func Parse(spec string, loc *time.Location) (Schedule, error) {
	if loc == nil {
		loc = time.UTC
	}
	spec = strings.TrimSpace(spec)
	if d, ok := strings.CutPrefix(spec, "@every "); ok {
		dur, err := time.ParseDuration(strings.TrimSpace(d))
		if err != nil {
			return nil, fmt.Errorf("cron: %q: %w", spec, err)
		}
		if dur < time.Second {
			return nil, fmt.Errorf("cron: %q: @every needs at least 1s", spec)
		}
		return every(dur), nil
	}
	if m, ok := macros[strings.ToLower(spec)]; ok {
		spec = m
	}
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: %q: want 5 fields (minute hour day month weekday) or a macro, got %d", spec, len(fields))
	}
	s := &spec5{loc: loc}
	var err error
	for i, f := range []*bitset{&s.minute, &s.hour, &s.dom, &s.month, &s.dow} {
		if *f, err = parseField(fields[i], bounds[i]); err != nil {
			return nil, fmt.Errorf("cron: %q: %s: %w", spec, bounds[i].name, err)
		}
	}
	if s.dow&(1<<7) != 0 { // 7 is Sunday too
		s.dow |= 1
	}
	s.domStar, s.dowStar = fields[2] == "*" || fields[2] == "?", fields[4] == "*" || fields[4] == "?"
	return s, nil
}

var macros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

type bound struct {
	name     string
	min, max int
	names    []string // index + min = value
}

var bounds = [5]bound{
	{name: "minute", min: 0, max: 59},
	{name: "hour", min: 0, max: 23},
	{name: "day of month", min: 1, max: 31},
	{name: "month", min: 1, max: 12, names: []string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}},
	{name: "day of week", min: 0, max: 7, names: []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}},
}

// bitset has bit v set when value v matches.
type bitset uint64

func (b bitset) has(v int) bool { return b&(1<<uint(v)) != 0 }

func parseField(f string, b bound) (bitset, error) {
	var set bitset
	for _, part := range strings.Split(f, ",") {
		rng, stepText, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("bad step %q", stepText)
			}
			step = n
		}
		lo, hi := b.min, b.max
		switch {
		case rng == "*" || rng == "?":
		case strings.Contains(rng, "-"):
			a, z, _ := strings.Cut(rng, "-")
			var err error
			if lo, err = value(a, b); err != nil {
				return 0, err
			}
			if hi, err = value(z, b); err != nil {
				return 0, err
			}
			if lo > hi {
				return 0, fmt.Errorf("range %q runs backwards", rng)
			}
		default:
			v, err := value(rng, b)
			if err != nil {
				return 0, err
			}
			lo = v
			if !hasStep {
				hi = v // a single value; "5/10" means 5 to max every 10
			}
		}
		for v := lo; v <= hi; v += step {
			set |= 1 << uint(v)
		}
	}
	if set == 0 {
		return 0, errors.New("matches nothing")
	}
	return set, nil
}

func value(s string, b bound) (int, error) {
	for i, n := range b.names {
		if strings.EqualFold(s, n) {
			return b.min + i, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < b.min || v > b.max {
		return 0, fmt.Errorf("%q is not in %d-%d", s, b.min, b.max)
	}
	return v, nil
}

type spec5 struct {
	minute, hour, dom, month, dow bitset
	domStar, dowStar              bool
	loc                           *time.Location
}

func (s *spec5) dayMatches(t time.Time) bool {
	dom, dow := s.dom.has(t.Day()), s.dow.has(int(t.Weekday()))
	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dow
	case s.dowStar:
		return dom
	default:
		return dom || dow
	}
}

// Next walks forward field by field, jumping a whole month, day, hour or
// minute whenever that field cannot match.
func (s *spec5) Next(after time.Time) time.Time {
	orig := after.Location()
	t := after.In(s.loc).Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		if !s.month.has(int(t.Month())) {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, s.loc)
			continue
		}
		if !s.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, s.loc)
			continue
		}
		if !s.hour.has(t.Hour()) {
			next := time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, s.loc)
			if !next.After(t) { // a DST fall-back repeats the hour; skip past it
				next = t.Add(time.Hour).Truncate(time.Hour)
			}
			t = next
			continue
		}
		if !s.minute.has(t.Minute()) || repeatedWallClock(t) {
			t = t.Add(time.Minute)
			continue
		}
		return t.In(orig)
	}
	return time.Time{}
}

// repeatedWallClock reports whether t's wall-clock time already happened an
// hour earlier: the second pass through the hour a DST fall-back repeats,
// which must not fire a daily job twice.
func repeatedWallClock(t time.Time) bool {
	p := t.Add(-time.Hour)
	return p.Hour() == t.Hour() && p.Minute() == t.Minute() && p.Day() == t.Day()
}

type every time.Duration

func (e every) Next(after time.Time) time.Time {
	d := int64(e)
	n := after.UnixNano()
	next := (n/d + 1) * d
	if n < 0 && n%d != 0 {
		next -= d // integer division rounds toward zero
	}
	return time.Unix(0, next).In(after.Location())
}
