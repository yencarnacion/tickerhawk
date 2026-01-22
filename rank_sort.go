package main

import "sort"

// Rank ordering per spec:
// ratio desc, then share desc, then askish desc.
func sortRank(rows []RankRow) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Ratio != b.Ratio {
			return a.Ratio > b.Ratio
		}
		if a.Share != b.Share {
			return a.Share > b.Share
		}
		if a.Askish != b.Askish {
			return a.Askish > b.Askish
		}
		return a.Symbol < b.Symbol
	})
}
