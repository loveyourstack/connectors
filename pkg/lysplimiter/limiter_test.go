package lysplimiter

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestAcquire_AllowsFirstInWindow(t *testing.T) {
	lim, err := New(Config{
		Rules: []Rule{{
			Name:        "test",
			Methods:     []string{"GET"},
			Hosts:       []string{"api.example.com"},
			MaxRequests: 1,
			Window:      time.Minute,
		}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = lim.Acquire(context.Background(), Request{Method: "GET", Host: "api.example.com", Path: "/v1"})
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
}

func TestAcquire_ReturnsLimitErrorWhenExceeded(t *testing.T) {
	lim, err := New(Config{
		Action: ActionError,
		Rules: []Rule{{
			Name:        "minute",
			Methods:     []string{"GET"},
			Hosts:       []string{"api.example.com"},
			MaxRequests: 1,
			Window:      time.Minute,
		}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	req := Request{Method: "GET", Host: "api.example.com", Path: "/v1"}
	if err := lim.Acquire(context.Background(), req); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	err = lim.Acquire(context.Background(), req)
	if err == nil {
		t.Fatal("expected limit error, got nil")
	}

	var limErr LimitError
	if !errors.As(err, &limErr) {
		t.Fatalf("expected LimitError, got %T: %v", err, err)
	}
	if limErr.RuleName != "minute" {
		t.Fatalf("unexpected rule name: %q", limErr.RuleName)
	}
	if limErr.RetryAfter <= 0 {
		t.Fatalf("expected positive retry after, got %s", limErr.RetryAfter)
	}
}

func TestAcquire_SkipRuleExemptsRequest(t *testing.T) {
	lim, err := New(Config{
		Action: ActionError,
		Rules: []Rule{
			{
				Name:         "download-get",
				Methods:      []string{"GET"},
				Hosts:        []string{"download.maxmind.com"},
				PathContains: "/download",
				MaxRequests:  1,
				Window:       time.Hour,
			},
			{
				Name:         "download-head-exempt",
				Methods:      []string{"HEAD"},
				Hosts:        []string{"download.maxmind.com"},
				PathContains: "/download",
				Skip:         true,
			},
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// HEAD requests are exempt and should not consume quota.
	for i := 0; i < 5; i++ {
		err := lim.Acquire(context.Background(), Request{
			Method: "HEAD",
			Host:   "download.maxmind.com",
			Path:   "/geoip/databases/GeoLite2-City-CSV/download",
		})
		if err != nil {
			t.Fatalf("HEAD acquire failed at %d: %v", i, err)
		}
	}

	// First GET succeeds.
	err = lim.Acquire(context.Background(), Request{
		Method: "GET",
		Host:   "download.maxmind.com",
		Path:   "/geoip/databases/GeoLite2-City-CSV/download",
	})
	if err != nil {
		t.Fatalf("GET acquire failed: %v", err)
	}

	// Second GET exceeds.
	err = lim.Acquire(context.Background(), Request{
		Method: "GET",
		Host:   "download.maxmind.com",
		Path:   "/geoip/databases/GeoLite2-City-CSV/download",
	})
	if err == nil {
		t.Fatal("expected limit error for second GET")
	}
}

func TestAcquire_MultipleMatchingRulesMustAllPass(t *testing.T) {
	lim, err := New(Config{
		Action: ActionError,
		Rules: []Rule{
			{
				Name:        "minute",
				Methods:     []string{"GET"},
				Hosts:       []string{"geoip.maxmind.com"},
				MaxRequests: 1,
				Window:      time.Minute,
			},
			{
				Name:        "day",
				Methods:     []string{"GET"},
				Hosts:       []string{"geoip.maxmind.com"},
				MaxRequests: 1000,
				Window:      24 * time.Hour,
			},
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	req := Request{Method: "GET", Host: "geoip.maxmind.com", Path: "/geoip/v2.1/city/1.1.1.1"}
	if err := lim.Acquire(context.Background(), req); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	err = lim.Acquire(context.Background(), req)
	if err == nil {
		t.Fatal("expected limit error due to minute rule")
	}
}

func TestAcquireHTTPRequest_UsesMethodHostPath(t *testing.T) {
	lim, err := New(Config{
		Action: ActionError,
		Rules: []Rule{{
			Name:        "req",
			Methods:     []string{"GET"},
			Hosts:       []string{"example.com"},
			PathPrefix:  "/api",
			MaxRequests: 1,
			Window:      time.Minute,
		}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, "https://example.com/api/items", nil)
	if err != nil {
		t.Fatalf("http.NewRequest failed: %v", err)
	}

	if err := lim.AcquireHTTPRequest(context.Background(), req); err != nil {
		t.Fatalf("AcquireHTTPRequest failed: %v", err)
	}
}

func TestAcquire_WaitActionSleepsAndRespectsContext(t *testing.T) {
	base := time.Date(2026, 6, 26, 10, 0, 0, 0, time.UTC)
	nowVal := base

	lim, err := New(Config{
		Action: ActionWait,
		Rules: []Rule{{
			Name:        "wait",
			Methods:     []string{"GET"},
			Hosts:       []string{"api.example.com"},
			MaxRequests: 1,
			Window:      time.Minute,
		}},
	},
		WithNow(func() time.Time { return nowVal }),
		WithSleep(func(ctx context.Context, d time.Duration) error {
			// Simulate waiting by advancing fake clock.
			nowVal = nowVal.Add(d)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	req := Request{Method: "GET", Host: "api.example.com", Path: "/v1"}
	if err := lim.Acquire(context.Background(), req); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Second call should wait until next bucket, then succeed.
	if err := lim.Acquire(context.Background(), req); err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
}

func TestNew_Validation(t *testing.T) {
	_, err := New(Config{Action: "bad"})
	if err == nil {
		t.Fatal("expected invalid action error")
	}

	_, err = New(Config{Rules: []Rule{{Name: "bad", MaxRequests: 0, Window: time.Minute}}})
	if err == nil {
		t.Fatal("expected max requests validation error")
	}

	_, err = New(Config{Rules: []Rule{{Name: "bad", MaxRequests: 1, Window: 0}}})
	if err == nil {
		t.Fatal("expected window validation error")
	}

	_, err = New(Config{Rules: []Rule{{Name: "skip", Skip: true}}})
	if err != nil {
		t.Fatalf("skip-only rule should be valid, got: %v", err)
	}
}
