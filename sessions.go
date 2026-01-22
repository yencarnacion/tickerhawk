package main

import (
	"fmt"
	"strings"
	"time"
)

func NYDateMinuteSession(tsMs int64, loc *time.Location) (date string, minute string, session string) {
	t := time.UnixMilli(tsMs).In(loc)
	date = t.Format("2006-01-02")
	minute = t.Format("15:04")
	session = SessionForNY(t)
	return
}

func SessionForNY(t time.Time) string {
	// Sessions in America/New_York:
	// PRE = 04:00:00 to 09:30:00
	// RTH = 09:30:00 to 16:00:00
	// AH  = 16:00:00 to 20:00:00
	// else CLOSED
	h, m, s := t.Clock()
	sec := h*3600 + m*60 + s

	preStart := 4*3600 + 0
	rthStart := 9*3600 + 30*60
	ahStart := 16 * 3600
	ahEnd := 20 * 3600

	switch {
	case sec >= preStart && sec < rthStart:
		return "PRE"
	case sec >= rthStart && sec < ahStart:
		return "RTH"
	case sec >= ahStart && sec < ahEnd:
		return "AH"
	default:
		return "CLOSED"
	}
}

// Parse "HH:MM" or "HH:MM:SS" into minutes since midnight.
func ParseSinceMinutes(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty time")
	}
	layout := "15:04"
	if strings.Count(s, ":") == 2 {
		layout = "15:04:05"
	}
	t, err := time.Parse(layout, s)
	if err != nil {
		return 0, err
	}
	return t.Hour()*60 + t.Minute(), nil
}

func MinuteStringToMinutes(minute string) (int, error) {
	return ParseSinceMinutes(minute) // same parsing rules
}
