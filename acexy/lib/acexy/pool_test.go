package acexy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// mockEngine returns a backend pointing at an httptest server that answers the AceStream
// middleware getstream JSON and counts how many getstream requests it received.
func mockEngine(t *testing.T, hits *int64) (Backend, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(hits, 1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"response":{"playback_url":"http://pb/x","stat_url":"http://st/x","command_url":"http://cmd/x"},"error":""}`)
	}))
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())
	return Backend{Scheme: "http", Host: u.Hostname(), Port: port}, srv.Close
}

// TestBackendPoolRoundRobin verifies new streams are spread evenly across the engine pool.
func TestBackendPoolRoundRobin(t *testing.T) {
	const N = 3
	var hits [N]int64
	var backends []Backend
	for i := 0; i < N; i++ {
		b, closeFn := mockEngine(t, &hits[i])
		defer closeFn()
		backends = append(backends, b)
	}
	a := &Acexy{Backends: backends, Endpoint: MPEG_TS_ENDPOINT, NoResponseTimeout: 5 * time.Second, ClientEvictionTimeout: time.Second}
	a.Init()

	for i := 0; i < N*3; i++ {
		id, err := NewAceID("", fmt.Sprintf("%040x", i))
		if err != nil {
			t.Fatalf("NewAceID: %v", err)
		}
		if _, err := a.FetchStream(id, nil); err != nil {
			t.Fatalf("FetchStream %d: %v", i, err)
		}
	}
	for i := 0; i < N; i++ {
		if got := atomic.LoadInt64(&hits[i]); got != 3 {
			t.Errorf("backend %d got %d getstream hits, want 3 (round-robin)", i, got)
		}
	}
}

// TestBackendPoolFailover verifies a dead backend is skipped and the next one serves the stream.
func TestBackendPoolFailover(t *testing.T) {
	var goodHits int64
	good, closeGood := mockEngine(t, &goodHits)
	defer closeGood()
	// a backend that always errors (closed server -> connection refused)
	deadSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	du, _ := url.Parse(deadSrv.URL)
	dport, _ := strconv.Atoi(du.Port())
	dead := Backend{Scheme: "http", Host: du.Hostname(), Port: dport}
	deadSrv.Close() // now refuses connections

	a := &Acexy{Backends: []Backend{dead, good}, Endpoint: MPEG_TS_ENDPOINT, NoResponseTimeout: 2 * time.Second, ClientEvictionTimeout: time.Second}
	a.Init()
	id, _ := NewAceID("", fmt.Sprintf("%040x", 1))
	if _, err := a.FetchStream(id, nil); err != nil {
		t.Fatalf("FetchStream should fail over to the good backend, got: %v", err)
	}
	if atomic.LoadInt64(&goodHits) == 0 {
		t.Error("good backend was never tried after the dead one failed")
	}
}

// TestBackendCooldownSpacing verifies cold-starts to one backend are spaced by BackendCooldown.
func TestBackendCooldownSpacing(t *testing.T) {
	var hits int64
	b, closeFn := mockEngine(t, &hits)
	defer closeFn()
	const cd = 150 * time.Millisecond
	a := &Acexy{Backends: []Backend{b}, BackendCooldown: cd, Endpoint: MPEG_TS_ENDPOINT, NoResponseTimeout: 5 * time.Second, ClientEvictionTimeout: time.Second}
	a.Init()
	start := time.Now()
	for i := 0; i < 4; i++ {
		id, _ := NewAceID("", fmt.Sprintf("%040x", i))
		if _, err := a.FetchStream(id, nil); err != nil {
			t.Fatalf("FetchStream %d: %v", i, err)
		}
	}
	// 4 cold-starts, one backend, 3 cooldown gaps -> >= 3*cd
	if elapsed := time.Since(start); elapsed < 3*cd {
		t.Errorf("cooldown not enforced: 4 cold-starts took %v, want >= %v", elapsed, 3*cd)
	}
}
