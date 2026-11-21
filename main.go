// firewatch scrapes Prometheus endpoints and ships the samples to Sentry
// Application Metrics.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

var httpClient = &http.Client{Timeout: 15 * time.Second}

type attr struct {
	Value any    `json:"value"`
	Type  string `json:"type"`
}

// item is one entry of a Sentry trace_metric envelope item.
type item struct {
	Timestamp  float64         `json:"timestamp"`
	TraceID    string          `json:"trace_id"`
	Name       string          `json:"name"`
	Value      float64         `json:"value"`
	Unit       string          `json:"unit,omitempty"`
	Type       string          `json:"type"`
	Attributes map[string]attr `json:"attributes"`
}

// ---- Sentry side ----

type sentry struct {
	url, auth, traceID string
	seq                atomic.Int64
}

func newSentry(dsn string) (*sentry, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	i := strings.LastIndex(u.Path, "/")
	if u.User.Username() == "" || u.Host == "" || i < 0 || u.Path[i+1:] == "" {
		return nil, errors.New("want https://<key>@<host>/<project-id>")
	}
	id := make([]byte, 16)
	rand.Read(id)
	return &sentry{
		url:     fmt.Sprintf("%s://%s%s/api/%s/envelope/", u.Scheme, u.Host, u.Path[:i], u.Path[i+1:]),
		auth:    "Sentry sentry_version=7, sentry_client=firewatch/1, sentry_key=" + u.User.Username(),
		traceID: hex.EncodeToString(id),
	}, nil
}

const batchSize = 1000

func (s *sentry) send(ctx context.Context, items []item) error {
	for len(items) > 0 {
		n := min(len(items), batchSize)
		if err := s.post(ctx, items[:n]); err != nil {
			return err
		}
		items = items[n:]
	}
	return nil
}

func (s *sentry) post(ctx context.Context, items []item) error {
	for i := range items {
		items[i].TraceID = s.traceID
		items[i].Attributes["sentry.timestamp.sequence"] = attr{s.seq.Add(1) - 1, "integer"}
	}
	payload, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		return err
	}
	var body bytes.Buffer
	fmt.Fprintf(&body, "{\"sent_at\":%q}\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&body, "{\"type\":\"trace_metric\",\"item_count\":%d,\"content_type\":\"application/vnd.sentry.items.trace-metric+json\"}\n", len(items))
	body.Write(payload)
	body.WriteByte('\n')

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-sentry-envelope")
	req.Header.Set("X-Sentry-Auth", s.auth)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// shortcut: a failed or rate-limited (429) batch is dropped, not retried;
	// add a retry queue if gaps during Sentry outages start to matter.
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("sentry: %s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}

// ---- Prometheus side ----

type scraper struct {
	url, instance string
	filter        *regexp.Regexp
	prev          map[string]float64 // last cumulative value per series
	primed        bool               // a scrape has succeeded, so prev is a baseline
}

func (s *scraper) scrape(ctx context.Context) ([]item, error) {
	ts := float64(time.Now().UnixMilli()) / 1e3
	up := func(v float64) item {
		return item{Timestamp: ts, Name: "up", Value: v, Type: "gauge",
			Attributes: map[string]attr{"instance": {s.instance, "string"}}}
	}
	fams, err := s.fetch(ctx)
	if err != nil {
		return []item{up(0)}, err
	}
	return append(s.convert(fams, ts), up(1)), nil
}

func (s *scraper) fetch(ctx context.Context) (map[string]*dto.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape: %s", resp.Status)
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	return p.TextToMetricFamilies(resp.Body)
}

// convert maps Prometheus families onto Sentry's three metric types.
// Prometheus counters are cumulative and Sentry counters are increments, so
// anything cumulative (counters, histogram/summary _sum, _count, _bucket) is
// sent as the delta since the previous scrape.
func (s *scraper) convert(fams map[string]*dto.MetricFamily, ts float64) []item {
	var out []item
	next := make(map[string]float64)

	emit := func(name, typ, unit string, v float64, attrs map[string]attr) {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return // not representable in JSON
		}
		out = append(out, item{Timestamp: ts, Name: name, Value: v, Unit: unit, Type: typ, Attributes: attrs})
	}
	count := func(name, unit string, cur float64, attrs map[string]attr) {
		key := seriesKey(name, attrs)
		next[key] = cur
		old, seen := s.prev[key]
		switch {
		case !seen && !s.primed:
			return // first scrape of this target: baseline only
		case seen && cur >= old:
			cur -= old
		} // otherwise the series is new or was reset, so its whole value is new
		if cur != 0 {
			emit(name, "counter", unit, cur, attrs)
		}
	}
	with := func(base map[string]attr, k, v string) map[string]attr {
		m := make(map[string]attr, len(base)+2)
		for bk, bv := range base {
			m[bk] = bv
		}
		m[k] = attr{v, "string"}
		return m
	}

	for name, fam := range fams {
		if s.filter != nil && !s.filter.MatchString(name) {
			continue
		}
		u := unitOf(name)
		for _, m := range fam.GetMetric() {
			base := map[string]attr{"instance": {s.instance, "string"}}
			for _, l := range m.GetLabel() {
				base[l.GetName()] = attr{l.GetValue(), "string"}
			}
			// each emitted item needs its own map: send() adds a sequence attribute
			own := func() map[string]attr { return with(base, "instance", base["instance"].Value.(string)) }

			switch fam.GetType() {
			case dto.MetricType_COUNTER:
				count(name, u, m.GetCounter().GetValue(), base)
			case dto.MetricType_GAUGE:
				emit(name, "gauge", u, m.GetGauge().GetValue(), base)
			case dto.MetricType_UNTYPED:
				emit(name, "gauge", u, m.GetUntyped().GetValue(), base)
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				count(name+"_count", "", float64(h.GetSampleCount()), own())
				count(name+"_sum", u, h.GetSampleSum(), own())
				for _, b := range h.GetBucket() {
					le := strconv.FormatFloat(b.GetUpperBound(), 'g', -1, 64)
					count(name+"_bucket", "", float64(b.GetCumulativeCount()), with(base, "le", le))
				}
			case dto.MetricType_SUMMARY:
				sm := m.GetSummary()
				count(name+"_count", "", float64(sm.GetSampleCount()), own())
				count(name+"_sum", u, sm.GetSampleSum(), own())
				for _, q := range sm.GetQuantile() {
					qs := strconv.FormatFloat(q.GetQuantile(), 'g', -1, 64)
					emit(name, "gauge", u, q.GetValue(), with(base, "quantile", qs))
				}
			}
		}
	}
	s.prev, s.primed = next, true
	return out
}

func seriesKey(name string, attrs map[string]attr) string {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		fmt.Fprintf(&b, "\xff%s\xff%v", k, attrs[k].Value)
	}
	return b.String()
}

func unitOf(name string) string {
	name = strings.TrimSuffix(name, "_total")
	switch {
	case strings.HasSuffix(name, "_seconds"):
		return "second"
	case strings.HasSuffix(name, "_bytes"):
		return "byte"
	}
	return ""
}

// ---- wiring ----

func main() {
	log.SetFlags(0)
	dsn, targets := os.Getenv("SENTRY_DSN"), strings.FieldsFunc(os.Getenv("TARGETS"), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' })
	if dsn == "" || len(targets) == 0 {
		log.Fatal("SENTRY_DSN and TARGETS (comma-separated Prometheus URLs) are required")
	}
	sn, err := newSentry(dsn)
	if err != nil {
		log.Fatalf("SENTRY_DSN: %v", err)
	}
	interval := time.Minute
	if v := os.Getenv("SCRAPE_INTERVAL"); v != "" {
		if interval, err = time.ParseDuration(v); err != nil || interval <= 0 {
			log.Fatalf("SCRAPE_INTERVAL: want a duration like 30s, got %q", v)
		}
	}
	var filter *regexp.Regexp
	if v := os.Getenv("METRIC_FILTER"); v != "" {
		if filter, err = regexp.Compile(v); err != nil {
			log.Fatalf("METRIC_FILTER: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for _, t := range targets {
		u, err := url.Parse(t)
		if err != nil || u.Host == "" {
			log.Fatalf("TARGETS: %q is not a URL like http://host:9100/metrics", t)
		}
		s := &scraper{url: t, instance: u.Host, filter: filter}
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for {
				items, err := s.scrape(ctx)
				if err != nil {
					log.Printf("%s: %v", s.instance, err)
				}
				if err := sn.send(ctx, items); err != nil {
					log.Printf("%s: %v", s.instance, err)
				} else {
					log.Printf("%s: shipped %d metrics", s.instance, len(items))
				}
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
			}
		}()
	}
	log.Printf("watching %d target(s) every %s", len(targets), interval)
	wg.Wait()
}
