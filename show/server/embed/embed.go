package embed

import "embed"

//go:embed *.html
//go:embed css
//go:embed js
var FS embed.FS
