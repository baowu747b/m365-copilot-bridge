package web

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

type conversation struct {
	ID             string    `json:"id"`
	AccountID      string    `json:"accountId"`
	ConversationID string    `json:"conversationId"`
	SessionID      string    `json:"sessionId"`
	ContentKey     string    `json:"contentKey,omitempty"`
	Title          string    `json:"title,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type sessionStore struct {
	mu   sync.Mutex
	path string
	data map[string]conversation
}

func openSessionStore() *sessionStore {
	path := os.Getenv("M365_SESSION_CACHE")
	if path == "" {
		path = filepath.Join(os.TempDir(), "m365-native-sessions.json")
	}
	s := &sessionStore{path: path, data: map[string]conversation{}}
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &s.data)
	}
	return s
}

// save serializes the in-memory table to disk. It takes a short lock only to
// copy, then writes outside the lock so the hot path never blocks concurrent
// readers on disk I/O. Previously saveLocked() wrote while holding s.mu, which
// both blocked reads and made every upsert an O(N) disk write that degraded to
// O(N^2) as the table grew (P0 from the 2026-10-02 review).
func (s *sessionStore) save() {
	s.mu.Lock()
	cp := make(map[string]conversation, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}
	s.mu.Unlock()
	b, _ := json.MarshalIndent(cp, "", "  ")
	_ = os.MkdirAll(filepath.Dir(s.path), 0o700)
	_ = os.WriteFile(s.path, b, 0o600)
}

func (s *sessionStore) list() []conversation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]conversation, 0, len(s.data))
	for _, v := range s.data {
		out = append(out, v)
	}
	return out
}

func (s *sessionStore) get(id string) (conversation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[id]
	return v, ok
}

// contentKeyTTL bounds how long a content-key → ChatHub conversation binding
// stays reusable. Beyond this window the upstream conversation is considered
// stale and a new one is opened, so the cache self-heals instead of pinning a
// dead conversation forever.
const contentKeyTTL = 30 * time.Minute

// getByContentKey returns the most recent conversation bound to (accountID,
// key) that is still within contentKeyTTL. It is an opt-in, client-supplied
// reuse key: identical content sent repeatedly can reuse the same ChatHub
// conversation and benefit from M365's own context/prefix reuse without
// polluting the per-thread sessionKey mapping.
func (s *sessionStore) getByContentKey(accountID, key string) (conversation, bool) {
	if key == "" {
		return conversation{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var best conversation
	found := false
	for _, v := range s.data {
		if v.AccountID != accountID || v.ContentKey != key {
			continue
		}
		if time.Since(v.UpdatedAt) > contentKeyTTL {
			continue
		}
		if !found || v.UpdatedAt.After(best.UpdatedAt) {
			best = v
			found = true
		}
	}
	return best, found
}

// getByContentKeyLocked is like getByContentKey but ignores contentKeyTTL and
// assumes s.mu is already held. It is used by upsert so a content-key upsert
// always merges into the existing row — never minting a new UUID — which keeps
// the table bounded regardless of TTL expiry.
func (s *sessionStore) getByContentKeyLocked(accountID, key string) (conversation, bool) {
	if key == "" {
		return conversation{}, false
	}
	var best conversation
	found := false
	for _, v := range s.data {
		if v.AccountID != accountID || v.ContentKey != key {
			continue
		}
		if !found || v.UpdatedAt.After(best.UpdatedAt) {
			best = v
			found = true
		}
	}
	return best, found
}

// getByContentKeyAny returns the most recent conversation bound to key across
// ALL accounts while still within contentKeyTTL. It is used to pin a request to
// the account that owns a content key BEFORE account resolution, so the
// round-robin allocator does not split reuse across different accounts (which
// would make getByContentKey miss and defeat the cache).
func (s *sessionStore) getByContentKeyAny(key string) (conversation, bool) {
	if key == "" {
		return conversation{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var best conversation
	found := false
	for _, v := range s.data {
		if v.ContentKey != key {
			continue
		}
		if time.Since(v.UpdatedAt) > contentKeyTTL {
			continue
		}
		if !found || v.UpdatedAt.After(best.UpdatedAt) {
			best = v
			found = true
		}
	}
	return best, found
}

func (s *sessionStore) upsert(v conversation) conversation {
	s.mu.Lock()
	// Merge into an existing content-key binding instead of minting a new
	// UUID on every call. Without this, any request carrying content_key but
	// no sessionKey created a brand-new row, so the table grew without bound
	// (P0 from the 2026-10-02 review).
	if v.ID == "" && v.ContentKey != "" && v.AccountID != "" {
		if existing, ok := s.getByContentKeyLocked(v.AccountID, v.ContentKey); ok {
			v.ID = existing.ID
		}
	}
	if v.ID == "" {
		v.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	s.data[v.ID] = v
	s.mu.Unlock()
	s.save()
	return v
}

func (s *sessionStore) delete(id string) bool {
	s.mu.Lock()
	if _, ok := s.data[id]; !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.data, id)
	s.mu.Unlock()
	s.save()
	return true
}

// deleteByAccount removes every cached conversation bound to the given account
// ID. Called when an account is deleted so stale session_key bindings don't keep
// returning 400 and don't pin a chat to a dead account (which would otherwise
// disable round-robin failover for that session).
func (s *sessionStore) deleteByAccount(accountID string) int {
	s.mu.Lock()
	removed := 0
	for k, v := range s.data {
		if v.AccountID == accountID {
			delete(s.data, k)
			removed++
		}
	}
	s.mu.Unlock()
	if removed > 0 {
		s.save()
	}
	return removed
}
