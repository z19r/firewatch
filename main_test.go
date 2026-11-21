package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/protobuf/proto"
)

const exposition = `# TYPE reqs_total counter
reqs_total{code="200"} %d
%s# TYPE cpu_seconds_total counter
cpu_seconds_total 1.5
# TYPE mem_bytes gauge
mem_bytes 42
# TYPE bad gauge
bad NaN
# TYPE lat_seconds histogram
lat_seconds_bucket{le="0.1"} %d
lat_seconds_bucket{le="+Inf"} %d
lat_seconds_sum 2.5
lat_seconds_count %d
`

func TestScrapeAndShip(t *testing.T) {
	reqs, extra := 10, ""
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, exposition, reqs, extra, reqs, reqs, reqs)
	}))
	defer prom.Close()

	var got []item
	var auth, itemHeader string
	fake := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("X-Sentry-Auth")
		if r.URL.Path != "/api/42/envelope/" {
			t.Errorf("path = %s", r.URL.Path)
		}
		sc := bufio.NewScanner(r.Body)
		sc.Buffer(nil, 1<<20)
		sc.Scan() // envelope header
		sc.Scan()
		itemHeader = sc.Text()
		sc.Scan()
		var p struct{ Items []item }
		if err := json.Unmarshal(sc.Bytes(), &p); err != nil {
			t.Error(err)
		}
		got = p.Items
	}))
	defer fake.Close()

	sn, err := newSentry(strings.Replace(fake.URL, "://", "://key@", 1) + "/42")
	if err != nil {
		t.Fatal(err)
	}
	s := &scraper{url: prom.URL, instance: "box:1"}
	run := func() map[string]item {
		t.Helper()
		items, err := s.scrape(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sn.send(context.Background(), items); err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf(`"item_count":%d,`, len(got)); !strings.Contains(itemHeader, want) {
			t.Errorf("item header %s lacks %s", itemHeader, want)
		}
		byName := map[string]item{}
		for _, it := range got {
			k := it.Name
			if le, ok := it.Attributes["le"]; ok {
				k += "/" + le.Value.(string)
			}
			byName[k] = it
		}
		return byName
	}

	// First scrape: gauges only, cumulative series just set a baseline.
	first := run()
	if len(first) != 2 || first["mem_bytes"].Value != 42 || first["up"].Value != 1 {
		t.Fatalf("first scrape = %+v", first)
	}
	if first["mem_bytes"].Unit != "byte" || first["mem_bytes"].Attributes["instance"].Value != "box:1" {
		t.Errorf("gauge = %+v", first["mem_bytes"])
	}
	if !strings.Contains(auth, "sentry_key=key") {
		t.Errorf("auth = %q", auth)
	}

	// Second scrape: deltas only, unchanged counters are omitted.
	reqs = 13
	second := run()
	for name, want := range map[string]float64{"reqs_total": 3, "lat_seconds_count": 3, "lat_seconds_bucket/0.1": 3, "lat_seconds_bucket/+Inf": 3, "mem_bytes": 42, "up": 1} {
		if it, ok := second[name]; !ok || it.Value != want {
			t.Errorf("%s = %+v, want %v", name, it, want)
		}
	}
	if len(second) != 6 {
		t.Errorf("second scrape sent %d metrics, want 6: %+v", len(second), second)
	}
	if second["reqs_total"].Type != "counter" || second["reqs_total"].Attributes["code"].Value != "200" {
		t.Errorf("counter = %+v", second["reqs_total"])
	}

	// Counter reset: the new value is the increment.
	reqs = 2
	if v := run()["reqs_total"].Value; v != 2 {
		t.Errorf("after reset reqs_total = %v, want 2", v)
	}

	// A series first seen after the baseline is new, so its whole value is sent.
	extra = "reqs_total{code=\"500\"} 4\n"
	if it := run()["reqs_total"]; it.Value != 4 || it.Attributes["code"].Value != "500" {
		t.Errorf("new series = %+v, want code=500 value 4", it)
	}

	// A series that vanishes for a scrape and comes back is sent as a delta,
	// not as its whole running total.
	extra = ""
	run()
	extra = "reqs_total{code=\"500\"} 6\n"
	if it := run()["reqs_total"]; it.Value != 2 || it.Attributes["code"].Value != "500" {
		t.Errorf("returning series = %+v, want code=500 delta 2", it)
	}

	// Target down: up=0 is still shipped.
	prom.Close()
	items, err := s.scrape(context.Background())
	if err == nil || len(items) != 1 || items[0].Name != "up" || items[0].Value != 0 {
		t.Errorf("down target: items=%+v err=%v", items, err)
	}
}

func TestBadDSN(t *testing.T) {
	for _, dsn := range []string{"", "https://host/1", "https://key@host", "https://key@host/"} {
		if _, err := newSentry(dsn); err == nil {
			t.Errorf("newSentry(%q) accepted", dsn)
		}
	}
}

func TestConfigAndUI(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "x 1") }))
	defer prom.Close()
	sink := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer sink.Close()
	dsn := strings.Replace(sink.URL, "://", "://key@", 1) + "/1"

	path := filepath.Join(t.TempDir(), "firewatch.yaml")
	yml := fmt.Sprintf("connections:\n  - target: %s\n    dsn: %s\n    interval: 30s\n    filter: ^x\n", prom.URL, dsn)
	if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	conns, err := loadConfig(path)
	if err != nil || len(conns) != 1 || conns[0] != (conn{prom.URL, dsn, "30s", "^x"}) {
		t.Fatalf("loadConfig = %+v, %v", conns, err)
	}
	os.WriteFile(path, []byte("connections:\n  - targt: x\n"), 0o600)
	if _, err := loadConfig(path); err == nil {
		t.Error("typo'd key accepted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &manager{ctx: ctx, path: path}
	if err := m.apply(conns, false); err != nil {
		t.Fatal(err)
	}
	kept := m.runners[0]
	ui := httptest.NewServer(m.handler("hunter2"))
	defer ui.Close()

	version := func() string {
		m.mu.Lock()
		defer m.mu.Unlock()
		return strconv.Itoa(m.version)
	}
	post := func(action string, form url.Values, mutate func(*http.Request)) int {
		t.Helper()
		if !form.Has("v") {
			form = maps.Clone(form)
			form.Set("v", version()) // as if posted from a freshly loaded page
		}
		req, _ := http.NewRequest(http.MethodPost, ui.URL+action, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth("me", "hunter2")
		if mutate != nil {
			mutate(req)
		}
		resp, err := http.DefaultTransport.RoundTrip(req) // no redirect following
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	add := url.Values{"i": {"new"}, "target": {prom.URL + "/other"}, "dsn": {dsn}}

	if code := post("/save", add, func(r *http.Request) { r.SetBasicAuth("me", "wrong") }); code != http.StatusUnauthorized {
		t.Errorf("wrong password: %d", code)
	}
	if code := post("/save", add, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }); code != http.StatusForbidden {
		t.Errorf("cross-site post: %d", code)
	}
	if code := post("/save", url.Values{"i": {"new"}, "target": {"ftp://x"}, "dsn": {dsn}}, nil); code != http.StatusBadRequest {
		t.Errorf("bad target: %d", code)
	}
	if code := post("/save", url.Values{"i": {"new"}, "target": {prom.URL}, "dsn": {dsn}, "interval": {"30s"}, "filter": {"^x"}}, nil); code != http.StatusBadRequest {
		t.Errorf("duplicate: %d", code)
	}
	if len(m.conns()) != 1 {
		t.Fatalf("rejected posts changed state: %+v", m.conns())
	}

	before := version()
	if code := post("/save", add, nil); code != http.StatusSeeOther {
		t.Fatalf("add: %d", code)
	}
	saved, err := loadConfig(path)
	if err != nil || len(saved) != 2 || saved[1].Target != prom.URL+"/other" {
		t.Fatalf("saved config = %+v, %v", saved, err)
	}
	if m.runners[0] != kept {
		t.Error("unchanged connection was restarted, losing its counter baseline")
	}
	if host := strings.TrimPrefix(prom.URL, "http://"); m.runners[1].sc.instance != host+"/other" {
		t.Errorf("second target on %s has instance %q, want it to include the path", host, m.runners[1].sc.instance)
	}

	// A page loaded before the add still numbers rows the old way: refuse its edits.
	stale := before
	if code := post("/delete", url.Values{"i": {"1"}, "v": {stale}}, nil); code != http.StatusConflict {
		t.Errorf("delete from stale page: %d", code)
	}
	if code := post("/save", url.Values{"i": {"new"}, "v": {stale}, "target": {prom.URL + "/third"}, "dsn": {dsn}}, nil); code != http.StatusConflict {
		t.Errorf("add from stale page: %d", code)
	}
	if len(m.conns()) != 2 {
		t.Fatalf("stale posts changed state: %+v", m.conns())
	}

	if code := post("/delete", url.Values{"i": {"0"}}, nil); code != http.StatusSeeOther {
		t.Fatalf("delete: %d", code)
	}
	if got := m.conns(); len(got) != 1 || got[0].Target != prom.URL+"/other" {
		t.Errorf("after delete: %+v", got)
	}

	req, _ := http.NewRequest(http.MethodGet, ui.URL+"/", nil)
	req.SetBasicAuth("me", "hunter2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), prom.URL+"/other") {
		t.Errorf("page: %d %s", resp.StatusCode, body)
	}
}

func TestSummary(t *testing.T) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(`# TYPE rpc_seconds summary
rpc_seconds{quantile="0.5"} 0.2
rpc_seconds_sum 7
rpc_seconds_count 5
`))
	if err != nil {
		t.Fatal(err)
	}
	s := &scraper{instance: "box:1"}
	first := s.convert(fams, 0)
	if len(first) != 1 || first[0].Type != "gauge" || first[0].Value != 0.2 || first[0].Attributes["quantile"].Value != "0.5" {
		t.Fatalf("first = %+v, want only the quantile gauge", first)
	}
	fams["rpc_seconds"].Metric[0].Summary.SampleCount = proto.Uint64(8)
	got := map[string]float64{}
	for _, it := range s.convert(fams, 0) {
		got[it.Name] = it.Value
	}
	if got["rpc_seconds_count"] != 3 || got["rpc_seconds"] != 0.2 {
		t.Errorf("second = %v, want count delta 3 and quantile 0.2", got)
	}
	if _, ok := got["rpc_seconds_sum"]; ok {
		t.Errorf("unchanged _sum was sent: %v", got)
	}
}

func TestSendBatches(t *testing.T) {
	for _, tc := range []struct {
		status, wantPosts, wantSent int
	}{
		{http.StatusOK, 3, 2*batchSize + 1},
		{http.StatusBadRequest, 3, batchSize + 1}, // a bad batch does not drop the rest
		{http.StatusTooManyRequests, 1, 0},        // rate limited: stop
	} {
		posts := 0
		fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if posts++; posts == 1 {
				w.WriteHeader(tc.status)
			}
		}))
		sn, err := newSentry(strings.Replace(fake.URL, "://", "://key@", 1) + "/42")
		if err != nil {
			t.Fatal(err)
		}
		items := make([]item, 2*batchSize+1)
		for i := range items {
			items[i].Attributes = map[string]attr{}
		}
		sent, err := sn.send(context.Background(), items)
		fake.Close()
		if posts != tc.wantPosts || sent != tc.wantSent || (err != nil) != (tc.status != http.StatusOK) {
			t.Errorf("status %d: posts=%d sent=%d err=%v, want %d posts, %d sent", tc.status, posts, sent, err, tc.wantPosts, tc.wantSent)
		}
	}
}

func TestNeedsPassword(t *testing.T) {
	for listen, want := range map[string]bool{
		"":               false,
		"127.0.0.1:8080": false,
		"[::1]:8080":     false,
		"localhost:8080": false,
		":8080":          true,
		"0.0.0.0:8080":   true,
		"10.0.0.5:8080":  true,
		"[::]:8080":      true,
		"example.com:80": true,
	} {
		if got := needsPassword(listen); got != want {
			t.Errorf("needsPassword(%q) = %v, want %v", listen, got, want)
		}
	}
}

func TestShutdownFinishesSend(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "x 1") }))
	defer prom.Close()
	got, release := make(chan struct{}, 1), make(chan struct{})
	sink := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		select {
		case got <- struct{}{}:
		default:
		}
		<-release
	}))
	defer sink.Close()

	ctx, cancel := context.WithCancel(context.Background())
	m := &manager{ctx: ctx}
	if err := m.apply([]conn{{Target: prom.URL, DSN: strings.Replace(sink.URL, "://", "://key@", 1) + "/1"}}, false); err != nil {
		t.Fatal(err)
	}
	<-got // a send is in flight
	cancel()
	close(release)
	m.wg.Wait()
	if r := m.runners[0]; r.shipped == 0 || r.lastErr != "" {
		t.Errorf("send cut off by shutdown: shipped=%d err=%q", r.shipped, r.lastErr)
	}
}

func TestSelfMetrics(t *testing.T) {
	r, err := conn{Target: "http://u:secret@h:9100/metrics", DSN: "https://k@h/1"}.build(nil)
	if err != nil {
		t.Fatal(err)
	}
	r.scrapes, r.failures, r.shipped = 3, 1, 40
	m := &manager{runners: []*runner{r}}
	rec := httptest.NewRecorder()
	m.metrics(rec, nil)
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	got := fams["firewatch_metrics_shipped_total"].GetMetric()[0]
	if got.GetCounter().GetValue() != 40 || fams["firewatch_scrape_failures_total"].GetMetric()[0].GetCounter().GetValue() != 1 {
		t.Errorf("wrong values: %v", fams)
	}
	for _, l := range got.GetLabel() {
		if strings.Contains(l.GetValue(), "secret") {
			t.Errorf("password leaked in label %s", l)
		}
	}
}

// Adding a second endpoint on a running host:port must relabel the first one
// too, or the same config gives different instance labels before and after a restart.
func TestInstanceRelabel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &manager{ctx: ctx}
	a := conn{Target: "http://127.0.0.1:1/metrics", DSN: "https://k@127.0.0.1:1/1", Interval: "1h"}
	b := conn{Target: "http://127.0.0.1:1/other", DSN: a.DSN, Interval: "1h"}
	if err := m.apply([]conn{a}, false); err != nil {
		t.Fatal(err)
	}
	if got := m.runners[0].sc.instance; got != "127.0.0.1:1" {
		t.Fatalf("alone: instance %q", got)
	}
	if err := m.apply([]conn{a, b}, false); err != nil {
		t.Fatal(err)
	}
	if got := m.runners[0].sc.instance; got != "127.0.0.1:1/metrics" {
		t.Errorf("shared: instance %q, want 127.0.0.1:1/metrics", got)
	}
	cancel()
	m.wg.Wait()
}

func TestTargetsFromEnv(t *testing.T) {
	t.Setenv("TARGETS", "http://a:1/metrics,\thttp://b:2/metrics\r\n")
	t.Setenv("SENTRY_DSN", "https://k@h/1")
	conns, err := loadConfig(filepath.Join(t.TempDir(), "none.yaml"))
	if err != nil || len(conns) != 2 || conns[1].Target != "http://b:2/metrics" {
		t.Fatalf("loadConfig = %q, %v", conns, err)
	}
}
