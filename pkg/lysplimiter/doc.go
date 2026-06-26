// Package lysplimiter provides a reusable, rule-based, in-process rate limiter
// for outbound API calls.
//
// The package is designed for external HTTP APIs where different request classes
// (method/host/path/tags) must follow different quotas, including exemption rules.
//
// Example:
//
//	lim, err := lysplimiter.New(lysplimiter.Config{
//		Action: lysplimiter.ActionError,
//		Rules: []lysplimiter.Rule{
//			{
//				Name:        "maxmind-download-get",
//				Methods:     []string{"GET"},
//				Hosts:       []string{"download.maxmind.com"},
//				PathContains: "/download",
//				MaxRequests: 30,
//				Window:      24 * time.Hour,
//			},
//			{
//				Name:         "maxmind-download-head-exempt",
//				Methods:      []string{"HEAD"},
//				Hosts:        []string{"download.maxmind.com"},
//				PathContains: "/download",
//				Skip:         true,
//			},
//		},
//	})
//	if err != nil {
//		return err
//	}
//
//	err = lim.Acquire(ctx, lysplimiter.Request{
//		Method: http.MethodGet,
//		Host:   "download.maxmind.com",
//		Path:   "/geoip/databases/GeoLite2-City-CSV/download",
//	})
//	if err != nil {
//		return err
//	}
package lysplimiter
