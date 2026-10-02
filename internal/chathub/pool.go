package chathub

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// warmupTargetHost is the ChatHub TLS endpoint. Pre-establishing TLS
// connections here lets the hot path skip DNS resolution, the TCP connect, and
// the TLS handshake.
const warmupTargetHost = "substrate.office.com:443"

// tlsConnPool keeps a small reserve of already-TLS-handshaked connections to
// the ChatHub endpoint. Every connection is single-use from the caller's point
// of view: it is handed out once for the WebSocket upgrade and then closed by
// chatWithHandlers. We never reuse a connection for a second chat, so this is
// fully compatible with ChatHub's single-use connection constraint. The only
// thing we reuse is the expensive DNS + TCP + TLS setup.
type tlsConnPool struct {
	mu     sync.Mutex
	idle   []*pooledTLS
	size   int
	maxAge time.Duration
	stop   chan struct{}
}

type pooledTLS struct {
	conn *tls.Conn
	born time.Time
}

// warmupPoolSize reads M365_WARMUP. 0 (the default) disables the pool; any
// positive value sets the number of pre-established connections kept warm per
// process. Negative values are treated as 0.
func warmupPoolSize() int {
	if v := os.Getenv("M365_WARMUP"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

func newTLSConnPool(size int) *tlsConnPool {
	p := &tlsConnPool{
		size:   size,
		maxAge: 20 * time.Second,
		stop:   make(chan struct{}),
	}
	if size > 0 {
		go p.maintain()
	}
	return p
}

// dialTLS establishes a fresh TLS connection to the ChatHub endpoint.
func dialTLS(ctx context.Context) (*tls.Conn, error) {
	d := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config:    &tls.Config{ServerName: "substrate.office.com"},
	}
	conn, err := d.DialContext(ctx, "tcp", warmupTargetHost)
	if err != nil {
		return nil, err
	}
	return conn.(*tls.Conn), nil
}

// get pops a warm connection if one is available and still fresh. The boolean
// reports whether a warm connection was handed out (a cache hit) so callers can
// record the metric and retry on a fresh dial if the warm conn died silently.
func (p *tlsConnPool) get() (*tls.Conn, bool) {
	if p == nil || p.size <= 0 {
		return nil, false
	}
	p.mu.Lock()
	for len(p.idle) > 0 {
		pc := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		if time.Since(pc.born) > p.maxAge {
			pc.conn.Close()
			continue
		}
		p.mu.Unlock()
		return pc.conn, true
	}
	p.mu.Unlock()
	return nil, false
}

// put returns a connection to the reserve, dropping it if the reserve is full.
func (p *tlsConnPool) put(c *tls.Conn) {
	if p == nil || p.size <= 0 {
		c.Close()
		return
	}
	p.mu.Lock()
	if len(p.idle) < p.size {
		p.idle = append(p.idle, &pooledTLS{conn: c, born: time.Now()})
	} else {
		c.Close()
	}
	p.mu.Unlock()
}

// maintain tops the reserve back up on a timer. If the endpoint is
// unreachable it simply retries on the next tick; it never blocks startup.
func (p *tlsConnPool) maintain() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			p.mu.Lock()
			for _, pc := range p.idle {
				pc.conn.Close()
			}
			p.idle = nil
			p.mu.Unlock()
			return
		case <-ticker.C:
			p.mu.Lock()
			need := p.size - len(p.idle)
			p.mu.Unlock()
			for i := 0; i < need; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				c, err := dialTLS(ctx)
				cancel()
				if err != nil {
					break
				}
				p.put(c)
			}
		}
	}
}

// Stop terminates the maintainer goroutine and closes idle connections.
func (p *tlsConnPool) Stop() {
	if p == nil {
		return
	}
	close(p.stop)
}
