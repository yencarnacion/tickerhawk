package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type watchlistFile struct {
	Watchlist []struct {
		Symbol string `yaml:"symbol"`
	} `yaml:"watchlist"`
}

func LoadWatchlist(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wf watchlistFile
	if err := yaml.Unmarshal(b, &wf); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(wf.Watchlist))
	out := make([]string, 0, len(wf.Watchlist))
	for _, it := range wf.Watchlist {
		s := strings.ToUpper(strings.TrimSpace(it.Symbol))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no symbols found in watchlist")
	}
	return out, nil
}
