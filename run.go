package main

import "time"

// RunContext is computed once at program start and stamped into every ClickHouse row.
type RunContext struct {
	ID      string
	StartNY time.Time
}
