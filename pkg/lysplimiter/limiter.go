package lysplimiter

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Action controls limiter behavior when a rule is exceeded.
type Action string

const (
	// ActionError returns a LimitError immediately when any matching quota is exceeded.
	ActionError Action = "error"
	// ActionWait blocks until capacity is available, honoring context cancellation.
	ActionWait Action = "wait"
)

// Request is the normalized request description used for matching rules.
type Request struct {
	Method string
	Host   string
	Path   string
	Tags   []string
}

// Rule defines one matching rule and, optionally, one quota.
//
// A rule with Skip=true acts as an exemption rule. If it matches a request,
// rate limiting is bypassed for that request.
type Rule struct {
	Name string

	Methods      []string
	Hosts        []string
	HostContains string
	PathPrefix   string
	PathContains string
	TagsAny      []string

	MaxRequests int
	Window      time.Duration
	Skip        bool
}

// Config configures a Limiter.
type Config struct {
	Rules []Rule

	// Action defaults to ActionError.
	Action Action

	// CleanupEvery controls stale-counter cleanup frequency and defaults to 5m.
	CleanupEvery time.Duration
}

// LimitError indicates that a matching rule has reached capacity.
type LimitError struct {
	RuleName    string
	MaxRequests int
	Window      time.Duration
	RetryAfter  time.Duration
}

func (e LimitError) Error() string {
	return fmt.Sprintf("rate limit exceeded (rule=%q max=%d window=%s retry_after=%s)", e.RuleName, e.MaxRequests, e.Window, e.RetryAfter)
}

// SleepFunc sleeps for d or returns early if ctx is canceled.
type SleepFunc func(ctx context.Context, d time.Duration) error

// Option customizes limiter internals.
type Option func(*Limiter)

// WithNow overrides time source. Mostly useful for tests.
func WithNow(now func() time.Time) Option {
	return func(l *Limiter) {
		if now != nil {
			l.now = now
		}
	}
}

// WithSleep overrides sleep behavior. Mostly useful for tests.
func WithSleep(sleep SleepFunc) Option {
	return func(l *Limiter) {
		if sleep != nil {
			l.sleep = sleep
		}
	}
}

type counterKey struct {
	ruleIdx int
	bucket  int64 // unix nanos of bucket start
}

// Limiter provides in-process fixed-window limiting with rule matching.
type Limiter struct {
	cfg Config

	mu          sync.Mutex
	counts      map[counterKey]int
	lastCleanup time.Time

	now   func() time.Time
	sleep SleepFunc
}

// New creates a new limiter with validated and normalized config.
func New(cfg Config, opts ...Option) (*Limiter, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	l := &Limiter{
		cfg:         normalized,
		counts:      make(map[counterKey]int),
		now:         time.Now,
		lastCleanup: time.Now(),
		sleep:       defaultSleep,
	}

	for _, opt := range opts {
		opt(l)
	}

	return l, nil
}

// Acquire enforces all matching quota rules for req.
//
// Behavior depends on Config.Action:
// - ActionError: returns LimitError when a rule is exceeded.
// - ActionWait: blocks until capacity is available (or context cancellation).
func (l *Limiter) Acquire(ctx context.Context, req Request) error {
	req = normalizeRequest(req)

	if l.isExempt(req) {
		return nil
	}

	matched := l.matchingQuotaRuleIndexes(req)
	if len(matched) == 0 {
		return nil
	}

	for {
		now := l.now()

		l.mu.Lock()
		l.cleanupLocked(now)

		exceeded, waitFor, rule := l.findExceededLocked(now, matched)
		if !exceeded {
			for _, idx := range matched {
				r := l.cfg.Rules[idx]
				k := counterKey{ruleIdx: idx, bucket: bucketStart(now, r.Window).UnixNano()}
				l.counts[k]++
			}
			l.mu.Unlock()
			return nil
		}

		l.mu.Unlock()

		if l.cfg.Action == ActionError {
			return LimitError{
				RuleName:    rule.Name,
				MaxRequests: rule.MaxRequests,
				Window:      rule.Window,
				RetryAfter:  waitFor,
			}
		}

		if err := l.sleep(ctx, waitFor); err != nil {
			return err
		}
	}
}

// AcquireHTTPRequest extracts request metadata and calls Acquire.
func (l *Limiter) AcquireHTTPRequest(ctx context.Context, req *http.Request, tags ...string) error {
	if req == nil {
		return fmt.Errorf("nil http request")
	}

	host := req.URL.Host
	if host == "" {
		host = req.Host
	}

	return l.Acquire(ctx, Request{
		Method: req.Method,
		Host:   host,
		Path:   req.URL.Path,
		Tags:   tags,
	})
}

func (l *Limiter) isExempt(req Request) bool {
	for _, r := range l.cfg.Rules {
		if r.Skip && ruleMatches(r, req) {
			return true
		}
	}
	return false
}

func (l *Limiter) matchingQuotaRuleIndexes(req Request) []int {
	out := make([]int, 0, len(l.cfg.Rules))
	for i, r := range l.cfg.Rules {
		if r.Skip {
			continue
		}
		if ruleMatches(r, req) {
			out = append(out, i)
		}
	}
	return out
}

func (l *Limiter) findExceededLocked(now time.Time, matched []int) (exceeded bool, waitFor time.Duration, rule Rule) {
	for _, idx := range matched {
		r := l.cfg.Rules[idx]
		k := counterKey{ruleIdx: idx, bucket: bucketStart(now, r.Window).UnixNano()}
		used := l.counts[k]
		if used >= r.MaxRequests {
			bucketEnd := bucketStart(now, r.Window).Add(r.Window)
			remaining := max(bucketEnd.Sub(now), time.Millisecond)
			return true, remaining, r
		}
	}

	return false, 0, Rule{}
}

func (l *Limiter) cleanupLocked(now time.Time) {
	if now.Sub(l.lastCleanup) < l.cfg.CleanupEvery {
		return
	}

	for k := range l.counts {
		r := l.cfg.Rules[k.ruleIdx]
		end := time.Unix(0, k.bucket).Add(r.Window)
		if !end.After(now) {
			delete(l.counts, k)
		}
	}

	l.lastCleanup = now
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Action == "" {
		cfg.Action = ActionError
	}
	if cfg.Action != ActionError && cfg.Action != ActionWait {
		return Config{}, fmt.Errorf("invalid action: %q", cfg.Action)
	}
	if cfg.CleanupEvery <= 0 {
		cfg.CleanupEvery = 5 * time.Minute
	}

	normalized := make([]Rule, 0, len(cfg.Rules))
	for i, r := range cfg.Rules {
		if r.Name == "" {
			r.Name = fmt.Sprintf("rule_%d", i+1)
		}
		if !r.Skip {
			if r.MaxRequests <= 0 {
				return Config{}, fmt.Errorf("rule %q: MaxRequests must be > 0", r.Name)
			}
			if r.Window <= 0 {
				return Config{}, fmt.Errorf("rule %q: Window must be > 0", r.Name)
			}
		}

		r.Methods = normalizeUpper(r.Methods)
		r.Hosts = normalizeLowerHosts(r.Hosts)
		r.HostContains = strings.ToLower(strings.TrimSpace(r.HostContains))
		r.PathPrefix = strings.TrimSpace(r.PathPrefix)
		r.PathContains = strings.TrimSpace(r.PathContains)
		r.TagsAny = normalizeLowerTrimmed(r.TagsAny)

		normalized = append(normalized, r)
	}

	cfg.Rules = normalized
	return cfg, nil
}

func normalizeRequest(req Request) Request {
	req.Method = strings.ToUpper(strings.TrimSpace(req.Method))
	req.Host = normalizeHost(req.Host)
	req.Path = strings.TrimSpace(req.Path)
	req.Tags = normalizeLowerTrimmed(req.Tags)
	return req
}

func ruleMatches(r Rule, req Request) bool {
	if len(r.Methods) > 0 && !containsString(r.Methods, req.Method) {
		return false
	}

	if len(r.Hosts) > 0 && !containsString(r.Hosts, req.Host) {
		return false
	}

	if r.HostContains != "" && !strings.Contains(req.Host, r.HostContains) {
		return false
	}

	if r.PathPrefix != "" && !strings.HasPrefix(req.Path, r.PathPrefix) {
		return false
	}

	if r.PathContains != "" && !strings.Contains(req.Path, r.PathContains) {
		return false
	}

	if len(r.TagsAny) > 0 && !sharesAny(req.Tags, r.TagsAny) {
		return false
	}

	return true
}

func bucketStart(t time.Time, window time.Duration) time.Time {
	return t.Truncate(window)
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func normalizeUpper(items []string) []string {
	out := make([]string, 0, len(items))
	for _, s := range items {
		t := strings.ToUpper(strings.TrimSpace(s))
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func normalizeLowerTrimmed(items []string) []string {
	out := make([]string, 0, len(items))
	for _, s := range items {
		t := strings.ToLower(strings.TrimSpace(s))
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func normalizeLowerHosts(items []string) []string {
	out := make([]string, 0, len(items))
	for _, h := range items {
		n := normalizeHost(h)
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

func normalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return ""
	}

	parsedHost, _, err := net.SplitHostPort(host)
	if err == nil {
		return parsedHost
	}

	// If split fails (most often because there is no port), keep input host.
	return host
}

func containsString(items []string, target string) bool {
	for _, s := range items {
		if s == target {
			return true
		}
	}
	return false
}

func sharesAny(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}

	lookup := make(map[string]struct{}, len(a))
	for _, s := range a {
		lookup[s] = struct{}{}
	}

	for _, s := range b {
		if _, ok := lookup[s]; ok {
			return true
		}
	}

	return false
}
