package main

import (
	"context"
	"strings"
	"time"

	massivews "github.com/massive-com/client-go/v2/websocket"
	"github.com/massive-com/client-go/v2/websocket/models"
)

type MassiveStreamConfig struct {
	APIKey  string
	FeedURL string // optional base feed override (e.g. wss://socket.massive.com)
	Symbols []string
	LocNY   *time.Location
	Log     *Logger
}

type MassiveStream struct {
	cfg     MassiveStreamConfig
	broker  *Broker
	metrics *Metrics
}

func NewMassiveStream(cfg MassiveStreamConfig, b *Broker, m *Metrics) *MassiveStream {
	return &MassiveStream{cfg: cfg, broker: b, metrics: m}
}

// NormalizeWSFeed accepts either base feed host or a full /stocks URL.
// The Massive websocket client expects Feed like "wss://socket.massive.com"
// and separately Market=Stocks (so it will connect to {Feed}/{Market}).
func NormalizeWSFeed(in string) string {
	in = strings.TrimSpace(in)
	if in == "" {
		return ""
	}
	in = strings.TrimRight(in, "/")
	l := strings.ToLower(in)
	// If someone passed ".../stocks", trim it (avoid "/stocks/stocks").
	if strings.HasSuffix(l, "/stocks") {
		in = in[:len(in)-len("/stocks")]
	}
	return in
}

func (s *MassiveStream) Run(ctx context.Context) {
	// The underlying client auto-reconnects and auto-resubscribes. We only
	// recreate it if we hit a fatal error via c.Error().
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cfg := massivews.Config{
			APIKey: s.cfg.APIKey,
			Feed:   massivews.RealTime,
			Market: massivews.Stocks,
			Log:    s.cfg.Log, // satisfies massivews.Logger (Debugf/Infof/Errorf)
			ReconnectCallback: func(err error) {
				if err != nil {
					s.metrics.ReconnectAttemptFailed()
					return
				}
				s.metrics.ReconnectSucceeded()
			},
		}
		if s.cfg.FeedURL != "" {
			cfg.Feed = massivews.Feed(s.cfg.FeedURL)
		}

		c, err := massivews.New(cfg)
		if err != nil {
			s.cfg.Log.Errorf("massivews.New failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Subscribe BEFORE Connect so reconnect paths automatically re-subscribe.
		if err := subscribeInChunks(c, massivews.StocksQuotes, s.cfg.Symbols, 100); err != nil {
			s.cfg.Log.Errorf("subscribe quotes failed: %v", err)
			c.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		if err := subscribeInChunks(c, massivews.StocksTrades, s.cfg.Symbols, 100); err != nil {
			s.cfg.Log.Errorf("subscribe trades failed: %v", err)
			c.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		if err := c.Connect(); err != nil {
			s.cfg.Log.Errorf("massivews connect failed: %v", err)
			c.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		s.cfg.Log.Infof("massive websocket connected (symbols=%d)", len(s.cfg.Symbols))

		// Event loop
		runOK := s.runClientLoop(ctx, c)
		c.Close()

		if runOK {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func (s *MassiveStream) runClientLoop(ctx context.Context, c *massivews.Client) bool {
	for {
		select {
		case <-ctx.Done():
			return true

		case err, ok := <-c.Error():
			if ok && err != nil {
				// fatal errors (e.g. auth failed)
				s.cfg.Log.Errorf("massive fatal error: %v", err)
			}
			return false

		case out, ok := <-c.Output():
			if !ok {
				// closed -> recreate client
				return false
			}
			switch msg := out.(type) {
			case models.EquityQuote:
				q := Quote{
					Symbol:    strings.ToUpper(msg.Symbol),
					Bid:       msg.BidPrice,
					Ask:       msg.AskPrice,
					TsEventMs: msg.Timestamp,
				}
				s.broker.HandleQuote(q)

			case models.EquityTrade:
				t := Trade{
					Symbol:    strings.ToUpper(msg.Symbol),
					Price:     msg.Price,
					Size:      msg.Size,
					TsEventMs: msg.Timestamp,
				}
				s.broker.HandleTrade(t)

			case models.ControlMessage:
				// can log auth/subscription statuses if desired
			default:
				// ignore other message types
			}
		}
	}
}

func subscribeInChunks(c *massivews.Client, topic massivews.Topic, syms []string, chunk int) error {
	if chunk <= 0 {
		chunk = 100
	}
	for i := 0; i < len(syms); i += chunk {
		j := i + chunk
		if j > len(syms) {
			j = len(syms)
		}
		if err := c.Subscribe(topic, syms[i:j]...); err != nil {
			return err
		}
	}
	return nil
}
