package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const exposition = `# TYPE reqs_total counter
reqs_total{code="200"} %d
# TYPE cpu_seconds_total counter
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
	reqs := 10
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, exposition, reqs, reqs, reqs, reqs)
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
		if err := sn.send(context.Background(), items); err != nil {
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
