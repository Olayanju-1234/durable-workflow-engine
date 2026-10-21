package record

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func encodeAll(t *testing.T, payloads ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, p := range payloads {
		rec, err := Encode([]byte(p))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		buf.Write(rec)
	}
	return buf.Bytes()
}

func scanAll(data []byte) ([]string, int64, error) {
	var got []string
	end, err := Scan(bufio.NewReader(bytes.NewReader(data)), func(p []byte) error {
		got = append(got, string(p))
		return nil
	})
	return got, end, err
}

func TestRoundTrip(t *testing.T) {
	data := encodeAll(t, "alpha", "", "gamma")
	got, end, err := scanAll(data)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if want := []string{"alpha", "", "gamma"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("got %q, want %q", got, want)
	}
	if end != int64(len(data)) {
		t.Fatalf("clean end %d, want %d", end, len(data))
	}
}

// A torn tail is the end of the usable data, not an error, and the clean end
// points at the last complete record so callers can truncate there.
func TestTornTailEndsScan(t *testing.T) {
	data := encodeAll(t, "one", "two")
	whole := int64(len(encodeAll(t, "one")))
	for cut := len(data) - 1; cut > int(whole); cut-- {
		got, end, err := scanAll(data[:cut])
		if err != nil {
			t.Fatalf("cut %d: unexpected error %v", cut, err)
		}
		if len(got) != 1 || end != whole {
			t.Fatalf("cut %d: got %q end %d, want [one] end %d", cut, got, end, whole)
		}
	}
}

func TestCorruptBodyIsHardError(t *testing.T) {
	data := encodeAll(t, "one", "two")
	data[HeaderSize] ^= 0xFF // flip a payload byte of the first record
	if _, _, err := scanAll(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestOversizeLengthIsHardError(t *testing.T) {
	data := encodeAll(t, "one")
	binary.BigEndian.PutUint32(data[0:4], MaxSize+1)
	if _, _, err := scanAll(data); err == nil || errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want a length error", err)
	}
	if _, err := Encode(make([]byte, MaxSize+1)); err == nil {
		t.Fatal("Encode accepted an oversize payload")
	}
}
