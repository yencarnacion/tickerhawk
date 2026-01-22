package main

import "math"

// epsilon for price comparisons (bid/ask equality checks).
const epsilon = 1e-3

// ClassifyBucket maps a trade price to the required buckets using NBBO.
//
// at_mid rule (deterministic):
//   mid = (bid+ask)/2
//   midEps = clamp(max(epsilon, spread*0.05), upper=spread*0.25)
//   if |price-mid| <= midEps => at_mid
//
// mid_to_ask: price > mid+midEps AND price < ask-epsilon
// mid_to_bid: price < mid-midEps AND price > bid+epsilon
//
// If bid/ask missing => bucket=at_mid, mid=0.
func ClassifyBucket(price, bid, ask float64) (bucket string, mid float64) {
	if bid <= 0 || ask <= 0 || ask < bid {
		return BucketAtMid, 0
	}

	// exact-ish checks
	if math.Abs(price-ask) <= epsilon {
		return BucketAtAsk, (bid + ask) / 2
	}
	if math.Abs(price-bid) <= epsilon {
		return BucketAtBid, (bid + ask) / 2
	}

	// outside NBBO
	if price > ask+epsilon {
		return BucketAboveAsk, (bid + ask) / 2
	}
	if price < bid-epsilon {
		return BucketBelowBid, (bid + ask) / 2
	}

	mid = (bid + ask) / 2
	spread := ask - bid

	// mid epsilon derived from spread (bounded)
	midEps := math.Max(epsilon, spread*0.05)
	if upper := spread * 0.25; midEps > upper {
		midEps = upper
	}

	if math.Abs(price-mid) <= midEps {
		return BucketAtMid, mid
	}

	if price > mid+midEps && price < ask-epsilon {
		return BucketMidToAsk, mid
	}
	if price < mid-midEps && price > bid+epsilon {
		return BucketMidToBid, mid
	}

	// If we land in a weird floating gap, fall back by closeness to edges.
	if math.Abs(price-ask) < math.Abs(price-bid) {
		return BucketMidToAsk, mid
	}
	return BucketMidToBid, mid
}
