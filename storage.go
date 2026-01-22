package main

import (
	"bufio"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type StorageConfig struct {
	DataDir   string
	FlushRaw  time.Duration
	Rotate    string
	MaxOpen   int
	LocNY     *time.Location
	Log       *Logger
}

type Storage struct {
	cfg     StorageConfig
	metrics *Metrics

	in chan TradeRecord

	// raw writer
	rawLRU *rawFileLRU

	// agg state (in-memory totals + deltas)
	aggMu   sync.RWMutex
	aggMap  map[AggKey]*aggCounter
	dirty   map[AggKey]struct{}

	// agg file handle (one per date in practice)
	aggFileMu sync.Mutex
	aggFile   *os.File
	aggBW     *bufio.Writer
	aggDate   string

	// agg file cache for query endpoints (tailing)
	queryCacheMu sync.Mutex
	queryCaches  map[string]*aggFileCache

	closeOnce sync.Once
}

type aggCounter struct {
	total AggCounts
	delta AggCounts
}

func NewStorage(cfg StorageConfig, m *Metrics) *Storage {
	if cfg.MaxOpen <= 0 {
		cfg.MaxOpen = 64
	}
	if cfg.FlushRaw <= 0 {
		cfg.FlushRaw = 250 * time.Millisecond
	}
	if strings.ToLower(cfg.Rotate) != "daily" {
		cfg.Rotate = "daily"
	}

	s := &Storage{
		cfg:         cfg,
		metrics:     m,
		in:          make(chan TradeRecord, 200_000),
		rawLRU:      newRawFileLRU(cfg.MaxOpen, cfg.Log),
		aggMap:      make(map[AggKey]*aggCounter, 64_000),
		dirty:       make(map[AggKey]struct{}, 4096),
		queryCaches: make(map[string]*aggFileCache),
	}
	return s
}

func (s *Storage) TryEnqueue(rec TradeRecord) bool {
	select {
	case s.in <- rec:
		return true
	default:
		return false
	}
}

func (s *Storage) Run(ctx context.Context) {
	// ensure base dirs exist
	_ = os.MkdirAll(filepath.Join(s.cfg.DataDir, "raw"), 0755)
	_ = os.MkdirAll(filepath.Join(s.cfg.DataDir, "agg"), 0755)

	// Attempt to resume today’s agg into in-memory totals (bonus behavior).
	s.resumeTodayAgg()

	rawFlushEvery := s.cfg.FlushRaw
	aggFlushEvery := rawFlushEvery
	if aggFlushEvery < 5*time.Second {
		aggFlushEvery = 5 * time.Second
	}

	t := time.NewTicker(rawFlushEvery)
	defer t.Stop()

	lastAggFlush := time.Now()
	lastMinute := time.Now().In(s.cfg.LocNY).Truncate(time.Minute)

	for {
		select {
		case <-ctx.Done():
			s.flushAll(true)
			return

		case rec := <-s.in:
			s.handleTrade(rec)

		case now := <-t.C:
			// flush raw writers frequently
			s.rawLRU.flushAll()

			// close idle raw files
			s.rawLRU.closeIdle(30 * time.Second)

			// agg flush: every aggFlushEvery and on minute boundary
			nowNY := now.In(s.cfg.LocNY)
			minNow := nowNY.Truncate(time.Minute)
			minChanged := !minNow.Equal(lastMinute)
			if minChanged {
				lastMinute = minNow
			}
			if minChanged || now.Sub(lastAggFlush) >= aggFlushEvery {
				s.flushAggDeltas()
				lastAggFlush = now
				// also flush agg writer to disk
				s.flushAggWriter()
			}
		}
	}
}

func (s *Storage) Close() {
	s.closeOnce.Do(func() {
		s.flushAll(true)
	})
}

func (s *Storage) flushAll(final bool) {
	// Drain quickly (best-effort) to reduce loss on shutdown.
Drain:
	for {
		select {
		case rec := <-s.in:
			s.handleTrade(rec)
		default:
			break Drain
		}
	}
	s.flushAggDeltas()
	s.flushAggWriter()
	s.rawLRU.flushAll()
	s.rawLRU.closeAll()
	s.closeAggFile()
}

func (s *Storage) handleTrade(rec TradeRecord) {
	// RAW write (truth)
	date, _, _ := NYDateMinuteSession(rec.TsEventMs, s.cfg.LocNY)
	s.writeRaw(date, rec.Symbol, rec)

	// AGG update
	date2, minute, session := NYDateMinuteSession(rec.TsEventMs, s.cfg.LocNY)
	key := AggKey{
		Date:    date2,
		Minute:  minute,
		Symbol:  rec.Symbol,
		Session: session,
	}

	s.aggMu.Lock()
	c := s.aggMap[key]
	if c == nil {
		c = &aggCounter{}
		s.aggMap[key] = c
	}
	c.total.AddBucket(rec.Bucket, 1)
	c.delta.AddBucket(rec.Bucket, 1)
	s.dirty[key] = struct{}{}
	s.aggMu.Unlock()
}

func (s *Storage) writeRaw(date, symbol string, rec TradeRecord) {
	sym := sanitizeSymbol(symbol)

	dir := filepath.Join(s.cfg.DataDir, "raw", date)
	_ = os.MkdirAll(dir, 0755)

	key := rawFileKey{Date: date, Symbol: sym}
	ent, err := s.rawLRU.getOrOpen(key, filepath.Join(dir, sym+".ndjson"))
	if err != nil {
		s.cfg.Log.Errorf("raw open %s %s: %v", date, sym, err)
		return
	}

	b, err := json.Marshal(rec)
	if err != nil {
		s.cfg.Log.Errorf("raw marshal %s: %v", sym, err)
		return
	}
	b = append(b, '\n')
	if _, err := ent.bw.Write(b); err != nil {
		s.cfg.Log.Errorf("raw write %s: %v", sym, err)
		return
	}
	ent.lastUsed = time.Now()
	s.rawLRU.touch(ent)
}

func sanitizeSymbol(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "/", "_")
	return s
}

// ---------- AGG flush/write ----------

func (s *Storage) flushAggDeltas() {
	// snapshot dirty keys to minimize lock hold
	s.aggMu.Lock()
	if len(s.dirty) == 0 {
		s.aggMu.Unlock()
		return
	}
	keys := make([]AggKey, 0, len(s.dirty))
	for k := range s.dirty {
		keys = append(keys, k)
	}
	// clear dirty now; new increments will re-add
	s.dirty = make(map[AggKey]struct{}, 4096)
	s.aggMu.Unlock()

	for _, k := range keys {
		s.aggMu.Lock()
		c := s.aggMap[k]
		if c == nil || c.delta.TradesTotal == 0 {
			s.aggMu.Unlock()
			continue
		}
		d := c.delta
		c.delta = AggCounts{} // reset delta
		s.aggMu.Unlock()

		if err := s.writeAggDelta(k, d); err != nil {
			s.cfg.Log.Errorf("agg write delta: %v", err)
		}
	}
}

func (s *Storage) writeAggDelta(k AggKey, d AggCounts) error {
	// open/rotate agg file by date
	if err := s.ensureAggFile(k.Date); err != nil {
		return err
	}
	rec := AggDeltaRecord{
		Date:    k.Date,
		Minute:  k.Minute,
		Session: k.Session,
		Symbol:  k.Symbol,

		TradesTotal: d.TradesTotal,
		AboveAsk:    d.AboveAsk,
		AtAsk:       d.AtAsk,
		MidToAsk:    d.MidToAsk,
		AtMid:       d.AtMid,
		MidToBid:    d.MidToBid,
		AtBid:       d.AtBid,
		BelowBid:    d.BelowBid,
	}

	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	s.aggFileMu.Lock()
	defer s.aggFileMu.Unlock()
	if s.aggBW == nil {
		return errors.New("agg writer missing")
	}
	_, err = s.aggBW.Write(b)
	return err
}

func (s *Storage) ensureAggFile(date string) error {
	s.aggFileMu.Lock()
	defer s.aggFileMu.Unlock()

	if s.aggDate == date && s.aggFile != nil && s.aggBW != nil {
		return nil
	}
	// rotate if open
	if s.aggFile != nil {
		_ = s.aggBW.Flush()
		_ = s.aggFile.Close()
		s.aggFile = nil
		s.aggBW = nil
		s.aggDate = ""
	}

	dir := filepath.Join(s.cfg.DataDir, "agg", date)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(dir, "minute.ndjson")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	s.aggFile = f
	s.aggBW = bufio.NewWriterSize(f, 1<<20) // 1MB buffer
	s.aggDate = date
	return nil
}

func (s *Storage) flushAggWriter() {
	s.aggFileMu.Lock()
	defer s.aggFileMu.Unlock()
	if s.aggBW != nil {
		_ = s.aggBW.Flush()
	}
}

func (s *Storage) closeAggFile() {
	s.aggFileMu.Lock()
	defer s.aggFileMu.Unlock()
	if s.aggBW != nil {
		_ = s.aggBW.Flush()
	}
	if s.aggFile != nil {
		_ = s.aggFile.Close()
	}
	s.aggFile = nil
	s.aggBW = nil
	s.aggDate = ""
}

// ---------- Resume today agg (bonus) ----------

func (s *Storage) resumeTodayAgg() {
	today := time.Now().In(s.cfg.LocNY).Format("2006-01-02")
	path := filepath.Join(s.cfg.DataDir, "agg", today, "minute.ndjson")
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// lines are small, but bump buffer just in case
	sc.Buffer(make([]byte, 0, 64*1024), 512*1024)

	var restored int
	for sc.Scan() {
		line := sc.Bytes()
		var r AggDeltaRecord
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}
		k := AggKey{Date: r.Date, Minute: r.Minute, Session: r.Session, Symbol: r.Symbol}
		s.aggMu.Lock()
		c := s.aggMap[k]
		if c == nil {
			c = &aggCounter{}
			s.aggMap[k] = c
		}
		// these are deltas in file, so add to totals
		c.total.TradesTotal += r.TradesTotal
		c.total.AboveAsk += r.AboveAsk
		c.total.AtAsk += r.AtAsk
		c.total.MidToAsk += r.MidToAsk
		c.total.AtMid += r.AtMid
		c.total.MidToBid += r.MidToBid
		c.total.AtBid += r.AtBid
		c.total.BelowBid += r.BelowBid
		s.aggMu.Unlock()
		restored++
	}
	if restored > 0 {
		s.cfg.Log.Infof("agg resume: restored %d delta rows from %s", restored, path)
	}
}

// ---------- In-memory queries ----------

func (s *Storage) GetAggTotals(key AggKey) (AggCounts, bool) {
	s.aggMu.RLock()
	defer s.aggMu.RUnlock()
	c := s.aggMap[key]
	if c == nil {
		return AggCounts{}, false
	}
	return c.total, true
}

// ---------- File-backed query cache for /rank/afterhours ----------

type aggFileCache struct {
	mu     sync.Mutex
	date   string
	path   string
	offset int64
	totals map[AggKey]AggCounts
}

func (s *Storage) getAggFileCache(date string) *aggFileCache {
	s.queryCacheMu.Lock()
	defer s.queryCacheMu.Unlock()
	c := s.queryCaches[date]
	if c == nil {
		c = &aggFileCache{
			date:   date,
			path:   filepath.Join(s.cfg.DataDir, "agg", date, "minute.ndjson"),
			totals: make(map[AggKey]AggCounts, 64_000),
		}
		s.queryCaches[date] = c
	}
	return c
}

// Ensure the cache has read the file up to EOF (tail-incremental).
func (s *Storage) updateAggCache(date string) error {
	c := s.getAggFileCache(date)
	c.mu.Lock()
	defer c.mu.Unlock()

	f, err := os.Open(c.path)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() < c.offset {
		// truncated/rotated; rebuild
		c.offset = 0
		c.totals = make(map[AggKey]AggCounts, 64_000)
	}

	if _, err := f.Seek(c.offset, 0); err != nil {
		return err
	}

	r := bufio.NewReaderSize(f, 1<<20)
	var readBytes int64

	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			readBytes += int64(len(line))
			var d AggDeltaRecord
			if json.Unmarshal(bytesTrimNL(line), &d) == nil {
				k := AggKey{Date: d.Date, Minute: d.Minute, Session: d.Session, Symbol: d.Symbol}
				cur := c.totals[k]
				cur.TradesTotal += d.TradesTotal
				cur.AboveAsk += d.AboveAsk
				cur.AtAsk += d.AtAsk
				cur.MidToAsk += d.MidToAsk
				cur.AtMid += d.AtMid
				cur.MidToBid += d.MidToBid
				cur.AtBid += d.AtBid
				cur.BelowBid += d.BelowBid
				c.totals[k] = cur
			}
		}
		if err != nil {
			// EOF expected
			break
		}
	}

	c.offset += readBytes
	return nil
}

func bytesTrimNL(b []byte) []byte {
	// remove trailing \n (and optional \r)
	if len(b) == 0 {
		return b
	}
	if b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}

func (s *Storage) RankAfterHoursFromFile(date string, sinceMinutes int, symbols []string, limit int) ([]RankRow, error) {
	if err := s.updateAggCache(date); err != nil {
		return nil, err
	}
	c := s.getAggFileCache(date)

	// read totals snapshot safely
	c.mu.Lock()
	totals := c.totals
	// no copy (for speed). We just won't mutate while iterating: we hold the lock.
	defer c.mu.Unlock()

	// AH ends at 20:00 ET per spec
	endMinutes := 20 * 60
	if sinceMinutes < 0 {
		sinceMinutes = 0
	}
	if sinceMinutes > endMinutes {
		sinceMinutes = endMinutes
	}

	out := make([]RankRow, 0, len(symbols))
	for _, sym := range symbols {
		var sum AggCounts
		for mm := sinceMinutes; mm < endMinutes; mm++ {
			h := mm / 60
			m := mm % 60
			minStr := fmt.Sprintf("%02d:%02d", h, m)
			k := AggKey{Date: date, Minute: minStr, Session: "AH", Symbol: sym}
			if v, ok := totals[k]; ok {
				sum.TradesTotal += v.TradesTotal
				sum.AboveAsk += v.AboveAsk
				sum.AtAsk += v.AtAsk
				sum.MidToAsk += v.MidToAsk
				sum.AtMid += v.AtMid
				sum.MidToBid += v.MidToBid
				sum.AtBid += v.AtBid
				sum.BelowBid += v.BelowBid
			}
		}
		if sum.TradesTotal == 0 {
			continue
		}
		askish := sum.Askish()
		bidish := sum.Bidish()
		total := sum.TradesTotal

		ratio := float64(askish) / float64(max64(1, bidish))
		share := float64(askish) / float64(max64(1, total))

		out = append(out, RankRow{
			Symbol: sym,
			Askish: askish,
			Bidish: bidish,
			Total:  total,
			Ratio:  ratio,
			Share:  share,
		})
	}

	sortRank(out)
	if limit <= 0 || limit > len(out) {
		limit = len(out)
	}
	return out[:limit], nil
}

// ---------- Raw file LRU ----------

type rawFileKey struct {
	Date   string
	Symbol string
}

type rawFileEntry struct {
	key      rawFileKey
	path     string
	f        *os.File
	bw       *bufio.Writer
	lastUsed time.Time
	elem     *list.Element
}

type rawFileLRU struct {
	maxOpen int
	ll      *list.List
	m       map[rawFileKey]*rawFileEntry
	log     *Logger
}

func newRawFileLRU(maxOpen int, log *Logger) *rawFileLRU {
	return &rawFileLRU{
		maxOpen: maxOpen,
		ll:      list.New(),
		m:       make(map[rawFileKey]*rawFileEntry, maxOpen*2),
		log:     log,
	}
}

func (l *rawFileLRU) getOrOpen(key rawFileKey, path string) (*rawFileEntry, error) {
	if e := l.m[key]; e != nil {
		return e, nil
	}

	// evict if needed
	for l.ll.Len() >= l.maxOpen {
		l.evictOne()
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	ent := &rawFileEntry{
		key:      key,
		path:     path,
		f:        f,
		bw:       bufio.NewWriterSize(f, 1<<20), // 1MB buffer per file
		lastUsed: time.Now(),
	}
	ent.elem = l.ll.PushFront(ent)
	l.m[key] = ent
	return ent, nil
}

func (l *rawFileLRU) touch(ent *rawFileEntry) {
	if ent.elem != nil {
		l.ll.MoveToFront(ent.elem)
	}
}

func (l *rawFileLRU) evictOne() {
	back := l.ll.Back()
	if back == nil {
		return
	}
	ent, _ := back.Value.(*rawFileEntry)
	if ent == nil {
		l.ll.Remove(back)
		return
	}
	_ = ent.bw.Flush()
	_ = ent.f.Close()
	delete(l.m, ent.key)
	l.ll.Remove(back)
}

func (l *rawFileLRU) flushAll() {
	for e := l.ll.Front(); e != nil; e = e.Next() {
		ent, _ := e.Value.(*rawFileEntry)
		if ent == nil {
			continue
		}
		_ = ent.bw.Flush()
	}
}

func (l *rawFileLRU) closeIdle(maxIdle time.Duration) {
	cut := time.Now().Add(-maxIdle)
	for e := l.ll.Back(); e != nil; {
		prev := e.Prev()
		ent, _ := e.Value.(*rawFileEntry)
		if ent != nil && ent.lastUsed.Before(cut) {
			_ = ent.bw.Flush()
			_ = ent.f.Close()
			delete(l.m, ent.key)
			l.ll.Remove(e)
		}
		e = prev
	}
}

func (l *rawFileLRU) closeAll() {
	for e := l.ll.Front(); e != nil; e = e.Next() {
		ent, _ := e.Value.(*rawFileEntry)
		if ent == nil {
			continue
		}
		_ = ent.bw.Flush()
		_ = ent.f.Close()
	}
	l.ll.Init()
	l.m = make(map[rawFileKey]*rawFileEntry, l.maxOpen*2)
}

// ---------- helpers ----------

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
