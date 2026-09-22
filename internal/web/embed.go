package web

import "embed"

// ui holds the dashboard. It is compiled into the binary, so the server ships
// as one self-contained file with no asset directory to carry alongside it.
//
// The UI references no CDN at all, so it works on a box with no internet —
// which is exactly the kind of box Yggdrasil is run on.
//
//go:embed webui/index.html
var ui embed.FS
