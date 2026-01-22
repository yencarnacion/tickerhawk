package main

type Quote struct {
	Symbol     string
	Bid        float64
	Ask        float64
	TsEventMs  int64
	TsIngestMs int64
}

type Trade struct {
	Symbol     string
	Price      float64
	Size       int64
	TsEventMs  int64
	TsIngestMs int64
}

type TradeRecord struct {
	TsEventMs  int64   `json:"ts_event_ms"`
	TsIngestMs int64   `json:"ts_ingest_ms"`
	Symbol     string  `json:"symbol"`
	Price      float64 `json:"price"`
	Size       int64   `json:"size"`
	Bid        float64 `json:"bid"`
	Ask        float64 `json:"ask"`
	Mid        float64 `json:"mid"`
	Bucket     string  `json:"bucket"`
}

const (
	BucketAboveAsk = "above_ask"
	BucketAtAsk    = "at_ask"
	BucketMidToAsk = "mid_to_ask"
	BucketAtMid    = "at_mid"
	BucketMidToBid = "mid_to_bid"
	BucketAtBid    = "at_bid"
	BucketBelowBid = "below_bid"
)

type AggCounts struct {
	TradesTotal int64 `json:"trades_total"`

	AboveAsk int64 `json:"above_ask"`
	AtAsk    int64 `json:"at_ask"`
	MidToAsk int64 `json:"mid_to_ask"`
	AtMid    int64 `json:"at_mid"`
	MidToBid int64 `json:"mid_to_bid"`
	AtBid    int64 `json:"at_bid"`
	BelowBid int64 `json:"below_bid"`
}

func (a *AggCounts) AddBucket(bucket string, n int64) {
	a.TradesTotal += n
	switch bucket {
	case BucketAboveAsk:
		a.AboveAsk += n
	case BucketAtAsk:
		a.AtAsk += n
	case BucketMidToAsk:
		a.MidToAsk += n
	case BucketAtMid:
		a.AtMid += n
	case BucketMidToBid:
		a.MidToBid += n
	case BucketAtBid:
		a.AtBid += n
	case BucketBelowBid:
		a.BelowBid += n
	default:
		// treat unknown as at_mid for continuity
		a.AtMid += n
	}
}

func (a AggCounts) Askish() int64 {
	return a.AtMid + a.MidToAsk + a.AtAsk + a.AboveAsk
}

func (a AggCounts) Bidish() int64 {
	return a.MidToBid + a.AtBid + a.BelowBid
}

type AggKey struct {
	Date    string // YYYY-MM-DD in America/New_York
	Minute  string // HH:MM in America/New_York
	Symbol  string
	Session string // PRE|RTH|AH|CLOSED
}

type AggDeltaRecord struct {
	Date    string `json:"date"`
	Minute  string `json:"minute"`
	Session string `json:"session"`
	Symbol  string `json:"symbol"`

	TradesTotal int64 `json:"trades_total"`

	AboveAsk int64 `json:"above_ask"`
	AtAsk    int64 `json:"at_ask"`
	MidToAsk int64 `json:"mid_to_ask"`
	AtMid    int64 `json:"at_mid"`
	MidToBid int64 `json:"mid_to_bid"`
	AtBid    int64 `json:"at_bid"`
	BelowBid int64 `json:"below_bid"`
}

type RankRow struct {
	Symbol string  `json:"symbol"`
	Askish int64   `json:"askish"`
	Bidish int64   `json:"bidish"`
	Total  int64   `json:"total"`
	Ratio  float64 `json:"ratio"`
	Share  float64 `json:"share"`
}
