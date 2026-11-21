// firewatch scrapes Prometheus endpoints and ships the samples to Sentry
// Application Metrics.
package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"

	"go.yaml.in/yaml/v2"

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

// send posts items in batches and reports how many Sentry accepted. A rejected
// batch does not stop the others, except on 429, where every further batch
// would be rejected too.
func (s *sentry) send(ctx context.Context, items []item) (int, error) {
	var errs []error
	sent := 0
	for len(items) > 0 {
		n := min(len(items), batchSize)
		if err := s.post(ctx, items[:n]); err == nil {
			sent += n
		} else {
			errs = append(errs, err)
			if errors.Is(err, errRateLimited) || ctx.Err() != nil {
				break
			}
		}
		items = items[n:]
	}
	return sent, errors.Join(errs...)
}

var errRateLimited = errors.New("rate limited")

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
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("sentry: %w", errRateLimited)
	}
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("sentry: %s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	io.Copy(io.Discard, resp.Body) // a drained body lets the connection be reused
	return nil
}

// ---- Prometheus side ----

type scraper struct {
	url, instance string
	filter        *regexp.Regexp
	prev          map[string]last // last cumulative value per series
	primed        bool            // a scrape has succeeded, so prev is a baseline
}

type last struct {
	v      float64
	missed int // scrapes in a row this series was absent
}

// keepMissing is how many scrapes a vanished series is remembered, so one that
// comes back is sent as a delta rather than as its lifetime total.
const keepMissing = 5

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
	next := make(map[string]last)

	emit := func(name, typ, unit string, v float64, attrs map[string]attr) {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return // not representable in JSON
		}
		out = append(out, item{Timestamp: ts, Name: name, Value: v, Unit: unit, Type: typ, Attributes: attrs})
	}
	count := func(name, unit string, cur float64, attrs map[string]attr) {
		key := seriesKey(name, attrs)
		next[key] = last{v: cur}
		p, seen := s.prev[key]
		old := p.v
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
		m := maps.Clone(base)
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

			switch fam.GetType() {
			case dto.MetricType_COUNTER:
				count(name, u, m.GetCounter().GetValue(), base)
			case dto.MetricType_GAUGE:
				emit(name, "gauge", u, m.GetGauge().GetValue(), base)
			case dto.MetricType_UNTYPED:
				emit(name, "gauge", u, m.GetUntyped().GetValue(), base)
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				// each emitted item needs its own map: send() adds a sequence attribute
				count(name+"_count", "", float64(h.GetSampleCount()), maps.Clone(base))
				count(name+"_sum", u, h.GetSampleSum(), maps.Clone(base))
				for _, b := range h.GetBucket() {
					le := strconv.FormatFloat(b.GetUpperBound(), 'g', -1, 64)
					count(name+"_bucket", "", float64(b.GetCumulativeCount()), with(base, "le", le))
				}
			case dto.MetricType_SUMMARY:
				sm := m.GetSummary()
				count(name+"_count", "", float64(sm.GetSampleCount()), maps.Clone(base))
				count(name+"_sum", u, sm.GetSampleSum(), maps.Clone(base))
				for _, q := range sm.GetQuantile() {
					qs := strconv.FormatFloat(q.GetQuantile(), 'g', -1, 64)
					emit(name, "gauge", u, q.GetValue(), with(base, "quantile", qs))
				}
			}
		}
	}
	for k, p := range s.prev {
		if _, ok := next[k]; !ok && p.missed < keepMissing {
			next[k] = last{p.v, p.missed + 1}
		}
	}
	s.prev, s.primed = next, true
	return out
}

func seriesKey(name string, attrs map[string]attr) string {
	var b strings.Builder
	b.WriteString(name)
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
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

// ---- config ----

// conn is one Prometheus endpoint -> Sentry project connection. All strings,
// so it round-trips through YAML and HTML forms unchanged and is comparable.
type conn struct {
	Target   string `yaml:"target"`
	DSN      string `yaml:"dsn"`
	Interval string `yaml:"interval,omitempty"`
	Filter   string `yaml:"filter,omitempty"`
}

// loadConfig reads the YAML file; if there is none, the environment is the config.
func loadConfig(path string) ([]conn, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		var out []conn
		for _, t := range strings.FieldsFunc(os.Getenv("TARGETS"), func(r rune) bool { return r == ',' || unicode.IsSpace(r) }) {
			out = append(out, conn{t, os.Getenv("SENTRY_DSN"), os.Getenv("SCRAPE_INTERVAL"), os.Getenv("METRIC_FILTER")})
		}
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg struct {
		Connections []conn `yaml:"connections"`
	}
	if err := yaml.UnmarshalStrict(b, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg.Connections, nil
}

func saveConfig(path string, conns []conn) error {
	b, err := yaml.Marshal(map[string][]conn{"connections": conns})
	if err != nil {
		return err
	}
	// temp file + rename, so a crash never leaves half a config (0600: it holds DSNs)
	f, err := os.CreateTemp(filepath.Dir(path), ".firewatch-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// ---- running connections ----

type runner struct {
	conn     conn
	sn       *sentry
	sc       *scraper
	interval time.Duration
	cancel   context.CancelFunc
	label    string // target with any password redacted, for /metrics

	mu                sync.Mutex
	last              time.Time
	lastErr           string
	shipped           int
	scrapes, failures int
}

// instanceOf labels a target: two endpoints on one host:port must not share a label.
func instanceOf(u *url.URL, shared map[string]bool) string {
	if shared[u.Host] {
		return u.Host + u.Path
	}
	return u.Host
}

// build makes a runner for c; shared holds the hosts used by more than one target.
func (c conn) build(shared map[string]bool) (*runner, error) {
	u, err := url.Parse(c.Target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("target %q is not a URL like http://host:9100/metrics", c.Target)
	}
	sn, err := newSentry(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("dsn: %w", err)
	}
	r := &runner{conn: c, sn: sn, sc: &scraper{url: c.Target, instance: instanceOf(u, shared)}, interval: time.Minute, label: u.Redacted()}
	if c.Interval != "" {
		if r.interval, err = time.ParseDuration(c.Interval); err != nil || r.interval < time.Second {
			return nil, fmt.Errorf("interval %q: want a duration of at least 1s, like 30s", c.Interval)
		}
	}
	if c.Filter != "" {
		if r.sc.filter, err = regexp.Compile(c.Filter); err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}
	}
	return r, nil
}

func (r *runner) run(ctx context.Context) {
	tick := time.NewTicker(r.interval)
	defer tick.Stop()
	for {
		items, err := r.sc.scrape(ctx)
		if ctx.Err() != nil {
			return // stopped mid-scrape: a failed scrape here is not the target's fault
		}
		// a scrape that finished is shipped even if we are stopping meanwhile
		sent, sendErr := r.sn.send(context.WithoutCancel(ctx), items)
		if err == nil {
			err = sendErr
		}
		r.mu.Lock()
		r.last, r.lastErr = time.Now(), ""
		r.scrapes++
		r.shipped += sent
		if err != nil {
			r.failures++
			r.lastErr = err.Error()
			log.Printf("%s: %v", r.sc.instance, err)
		} else {
			log.Printf("%s: shipped %d metrics", r.sc.instance, len(items))
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

type manager struct {
	ctx  context.Context
	path string

	mu      sync.Mutex
	runners []*runner
	version int // bumped by every apply, so a stale page's edits can be refused

	edit sync.Mutex // serialises web UI changes
	wg   sync.WaitGroup
}

// apply validates conns and makes them the running set, all or nothing.
// Unchanged connections keep running, so they keep their counter baselines.
func (m *manager) apply(conns []conn, persist bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := make(map[conn]*runner, len(m.runners))
	for _, r := range m.runners {
		old[r.conn] = r
	}
	first, shared := map[string]string{}, map[string]bool{}
	for _, c := range conns {
		if u, err := url.Parse(c.Target); err == nil {
			if t, ok := first[u.Host]; !ok {
				first[u.Host] = c.Target
			} else if t != c.Target {
				shared[u.Host] = true
			}
		}
	}
	next := make(map[conn]*runner, len(conns))
	for i, c := range conns {
		if next[c] != nil {
			return fmt.Errorf("connection %d duplicates an earlier one", i+1)
		}
		// reuse a running connection only if its label is unchanged
		if r := old[c]; r != nil {
			if u, _ := url.Parse(c.Target); r.sc.instance == instanceOf(u, shared) {
				next[c] = r
				continue
			}
		}
		r, err := c.build(shared)
		if err != nil {
			return fmt.Errorf("connection %d: %w", i+1, err)
		}
		next[c] = r
	}
	if persist {
		if err := saveConfig(m.path, conns); err != nil {
			return fmt.Errorf("saving config: %w", err)
		}
	}
	for c, r := range old {
		if next[c] != r {
			r.cancel()
		}
	}
	m.runners = m.runners[:0:0]
	for _, c := range conns {
		r := next[c]
		if r.cancel == nil {
			var ctx context.Context
			ctx, r.cancel = context.WithCancel(m.ctx)
			m.wg.Go(func() { r.run(ctx) })
		}
		m.runners = append(m.runners, r)
	}
	m.version++
	return nil
}

func (m *manager) conns() []conn {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]conn, len(m.runners))
	for i, r := range m.runners {
		out[i] = r.conn
	}
	return out
}

// metrics serves firewatch's own health in the Prometheus text format.
func (m *manager) metrics(w http.ResponseWriter, _ *http.Request) {
	type sample struct {
		conn, target                     string
		scrapes, failures, shipped, last float64
	}
	m.mu.Lock()
	samples := make([]sample, len(m.runners))
	for i, r := range m.runners {
		r.mu.Lock()
		samples[i] = sample{fmt.Sprintf("%02d", i+1), r.label, float64(r.scrapes), float64(r.failures), float64(r.shipped), 0}
		if !r.last.IsZero() {
			samples[i].last = float64(r.last.UnixMilli()) / 1000
		}
		r.mu.Unlock()
	}
	m.mu.Unlock()

	esc := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP firewatch_connections Configured connections.\n# TYPE firewatch_connections gauge\nfirewatch_connections %d\n", len(samples))
	for _, f := range []struct {
		name, typ, help string
		val             func(sample) float64
	}{
		{"firewatch_scrapes_total", "counter", "Scrape-and-ship cycles run.", func(s sample) float64 { return s.scrapes }},
		{"firewatch_scrape_failures_total", "counter", "Cycles where the scrape or the send to Sentry failed.", func(s sample) float64 { return s.failures }},
		{"firewatch_metrics_shipped_total", "counter", "Metric samples accepted by Sentry.", func(s sample) float64 { return s.shipped }},
		{"firewatch_last_scrape_timestamp_seconds", "gauge", "Unix time of the last cycle, 0 before the first.", func(s sample) float64 { return s.last }},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.typ)
		for _, s := range samples {
			fmt.Fprintf(w, "%s{connection=\"%s\",target=\"%s\"} %s\n", f.name, s.conn, esc.Replace(s.target), strconv.FormatFloat(f.val(s), 'g', -1, 64))
		}
	}
}

// needsPassword reports whether a UI on listen is reachable from other hosts.
// The UI can read DSNs and make this host fetch URLs: never expose it open.
func needsPassword(listen string) bool {
	if listen == "" {
		return false
	}
	host, _, _ := net.SplitHostPort(listen)
	ip := net.ParseIP(host)
	return host != "localhost" && (ip == nil || !ip.IsLoopback())
}

func main() {
	log.SetFlags(0)
	path := cmp.Or(os.Getenv("CONFIG"), "firewatch.yaml")
	conns, err := loadConfig(path)
	if err != nil {
		log.Fatal(err)
	}
	listen, password := os.Getenv("LISTEN"), os.Getenv("UI_PASSWORD")
	if len(conns) == 0 && listen == "" {
		log.Fatalf("nothing to do: no connections in %s, no SENTRY_DSN + TARGETS, and no LISTEN for the web UI", path)
	}
	if password == "" && needsPassword(listen) {
		log.Fatal("UI_PASSWORD is required unless LISTEN is a loopback address like 127.0.0.1:8080")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	m := &manager{ctx: ctx, path: path}
	if addr := os.Getenv("METRICS_LISTEN"); addr != "" {
		// bind before the runners start, so a self-scrape doesn't fail on startup
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /metrics", m.metrics)
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
		go func() { log.Fatal(srv.Serve(ln)) }()
		log.Printf("prometheus metrics on %s/metrics", addr)
	}
	if err := m.apply(conns, false); err != nil {
		log.Fatal(err)
	}
	log.Printf("watching %d connection(s)", len(conns))
	if listen != "" {
		srv := &http.Server{Addr: listen, Handler: m.handler(password), ReadHeaderTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
		go func() { log.Fatal(srv.ListenAndServe()) }()
		log.Printf("web UI on %s", listen)
	}
	<-ctx.Done()
	m.wg.Wait()
}
