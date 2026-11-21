package main

import (
	"crypto/subtle"
	_ "embed"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
)

//go:embed ui.html
var uiHTML string

// Big Shoulders Display (latin subset), SIL Open Font License 1.1.
//
//go:embed display.woff2
var displayFont []byte

var page = template.Must(template.New("ui").Parse(uiHTML))

type row struct {
	I    int
	N    string // "01", "02", ...
	Host string
	conn
	State  string // "start", "sight" or "lost"
	Detail string
}

func (m *manager) handler(password string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) { m.render(w, nil) })
	mux.HandleFunc("GET /display.woff2", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "font/woff2")
		w.Header().Set("Cache-Control", "max-age=86400")
		w.Write(displayFont)
	})
	mux.HandleFunc("POST /save", func(w http.ResponseWriter, r *http.Request) {
		conns := m.conns()
		c := conn{}
		for name, dst := range map[string]*string{"target": &c.Target, "dsn": &c.DSN, "interval": &c.Interval, "filter": &c.Filter} {
			*dst = strings.TrimSpace(r.FormValue(name))
		}
		if i, err := strconv.Atoi(r.FormValue("i")); r.FormValue("i") == "new" {
			conns = append(conns, c)
		} else if err != nil || i < 0 || i >= len(conns) {
			http.Error(w, "no such connection", http.StatusBadRequest)
			return
		} else {
			conns[i] = c
		}
		m.update(w, r, conns)
	})
	mux.HandleFunc("POST /delete", func(w http.ResponseWriter, r *http.Request) {
		conns := m.conns()
		i, err := strconv.Atoi(r.FormValue("i"))
		if err != nil || i < 0 || i >= len(conns) {
			http.Error(w, "no such connection", http.StatusBadRequest)
			return
		}
		m.update(w, r, append(conns[:i], conns[i+1:]...))
	})

	// browsers attach basic-auth credentials to cross-site form posts
	h := http.NewCrossOriginProtection().Handler(mux)
	if password == "" {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pw, _ := r.BasicAuth(); subtle.ConstantTimeCompare([]byte(pw), []byte(password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="firewatch"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (m *manager) update(w http.ResponseWriter, r *http.Request, conns []conn) {
	if err := m.apply(conns, true); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		m.render(w, err)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (m *manager) render(w http.ResponseWriter, problem error) {
	m.mu.Lock()
	rows := make([]row, len(m.runners))
	for i, r := range m.runners {
		r.mu.Lock()
		rows[i] = row{I: i, N: fmt.Sprintf("%02d", i+1), Host: r.sc.instance, conn: r.conn, State: "start"}
		switch {
		case r.lastErr != "":
			rows[i].State, rows[i].Detail = "lost", r.lastErr
		case !r.last.IsZero():
			rows[i].State = "sight"
			rows[i].Detail = fmt.Sprintf("%d metrics shipped · last scrape %s", r.shipped, r.last.Format("15:04:05"))
		}
		r.mu.Unlock()
	}
	m.mu.Unlock()
	page.Execute(w, map[string]any{"Rows": rows, "Path": m.path, "Err": problem})
}
