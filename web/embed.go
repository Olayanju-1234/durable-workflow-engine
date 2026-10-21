// Package web serves the operational dashboard. The assets in dashboard/ are
// compiled into the binary with go:embed, so `workflowd` stays a single file
// with no static-asset directory to deploy alongside it — the same
// single-binary promise as the rest of the engine.
//
// The dashboard is deliberately dependency-free: hand-written HTML, CSS and
// vanilla JS with no build step, no bundler, no npm. It talks to the JSON API
// in internal/server (see httpapi.go), which is served on the same listener.
//
// site/ (the public landing page and developer documentation) lives beside
// dashboard/ but is NOT embedded — it is a static site deployed separately.
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed dashboard
var assets embed.FS

// FS returns the dashboard assets rooted at dashboard/, so "index.html"
// resolves without the directory prefix.
func FS() fs.FS {
	sub, err := fs.Sub(assets, "dashboard")
	if err != nil {
		// Unreachable: the embed directive guarantees the directory exists, and
		// a bad pattern is a compile-time error.
		panic("web: embedded dashboard missing: " + err.Error())
	}
	return sub
}

// Handler serves the dashboard. Unknown paths fall back to index.html so the
// single-page app owns its own routing. Static assets are read-only, so any
// method but GET/HEAD gets a 405 instead of a misleading 200 + HTML shell.
func Handler() http.Handler {
	files := FS()
	fileServer := http.FileServer(http.FS(files))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		path := r.URL.Path
		if path == "/" {
			fileServer.ServeHTTP(w, r)
			return
		}
		if _, err := fs.Stat(files, path[1:]); err != nil {
			// Not a real asset: hand the SPA its shell rather than a 404.
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})
}
