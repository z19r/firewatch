package main

import (
	"crypto/subtle"
	_ "embed"
	"errors"
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
		c := conn{}
		for name, dst := range map[string]*string{"target": &c.Target, "dsn": &c.DSN, "interval": &c.Interval, "filter": &c.Filter} {
			*dst = strings.TrimSpace(r.FormValue(name))
		}
		m.change(w, r, func(conns []conn) ([]conn, error) {
			if r.FormValue("i") == "new" {
				return append(conns, c), nil
			}
			i, err := rowIndex(r, conns)
			if err != nil {
				return nil, err
			}
			conns[i] = c
			return conns, nil
		})
	})
	mux.HandleFunc("POST /delete", func(w http.ResponseWriter, r *http.Request) {
		m.change(w, r, func(conns []conn) ([]conn, error) {
			i, err := rowIndex(r, conns)
			if err != nil {
				return nil, err
			}
			return append(conns[:i], conns[i+1:]...), nil
		})
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

func rowIndex(r *http.Request, conns []conn) (int, error) {
	i, err := strconv.Atoi(r.FormValue("i"))
	if err != nil || i < 0 || i >= len(conns) {
		return 0, errors.New("no such connection")
	}
	return i, nil
}

// change applies one edit from the page. Rows are addressed by position, so an
// edit from a page older than the last change could hit the wrong row: refuse it.
func (m *manager) change(w http.ResponseWriter, r *http.Request, edit func([]conn) ([]conn, error)) {
	m.edit.Lock()
	defer m.edit.Unlock()
	m.mu.Lock()
	v := m.version
	m.mu.Unlock()
	if r.FormValue("v") != strconv.Itoa(v) {
		w.WriteHeader(http.StatusConflict)
		m.render(w, errors.New("the connections changed since that page was loaded; below is the current list, try again"))
		return
	}
	conns, err := edit(m.conns())
	if err == nil {
		err = m.apply(conns, true)
	}
	if err != nil {
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
	v := m.version
	m.mu.Unlock()
	page.Execute(w, map[string]any{"Rows": rows, "Path": m.path, "Err": problem, "V": v})
}
