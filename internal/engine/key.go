package engine

// key.go —— API Key 管理：生成/删除/校验/过期，持久化到 KV（前缀 k:）

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// APIKey 访问密钥
type APIKey struct {
	Key       string `json:"key"`       // 密钥值（32 位 hex）
	Name      string `json:"name"`      // 备注
	CreatedAt int64  `json:"createdAt"` // 创建时间（毫秒）
	ExpiresAt int64  `json:"expiresAt"` // 过期时间（毫秒），0=永不过期
}

var (
	ErrKeyNotFound = errors.New("key 不存在")
)

// keyCache 内存缓存（校验走内存，无 IO）
type keyCache struct {
	mu   sync.RWMutex
	keys map[string]*APIKey
}

func newKeyCache() *keyCache {
	return &keyCache{keys: make(map[string]*APIKey)}
}

func (c *keyCache) put(k *APIKey) {
	c.mu.Lock()
	c.keys[k.Key] = k
	c.mu.Unlock()
}

func (c *keyCache) del(key string) {
	c.mu.Lock()
	delete(c.keys, key)
	c.mu.Unlock()
}

// has 是否存在（不校验过期：过期 key 也应能删除）
func (c *keyCache) has(key string) bool {
	c.mu.RLock()
	_, ok := c.keys[key]
	c.mu.RUnlock()
	return ok
}

// valid 校验存在且未过期
func (c *keyCache) valid(key string, now int64) bool {
	c.mu.RLock()
	k, ok := c.keys[key]
	c.mu.RUnlock()
	if !ok {
		return false
	}
	return k.ExpiresAt == 0 || now < k.ExpiresAt
}

func (c *keyCache) list() []*APIKey {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*APIKey, 0, len(c.keys))
	for _, k := range c.keys {
		out = append(out, k)
	}
	return out
}

// ---------- Engine 接口 ----------

// loadKeys 启动时加载全部 key 到内存
func (e *Engine) loadKeys() error {
	for _, k := range e.st.ListAPIKeys() {
		e.keyCache.put(k)
	}
	return nil
}

// CreateKey 创建密钥（expiresAt 毫秒，0=永不过期）；返回新 key
func (e *Engine) CreateKey(name string, expiresAt int64) (*APIKey, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	k := &APIKey{
		Key:       hex.EncodeToString(buf),
		Name:      name,
		CreatedAt: time.Now().UnixMilli(),
		ExpiresAt: expiresAt,
	}
	if err := e.st.PutAPIKey(k); err != nil {
		return nil, err
	}
	e.keyCache.put(k)
	log.Printf("已创建 API Key: %s...（%s，%s）", k.Key[:8], keyDesc(k), name)
	return k, nil
}

// DeleteKey 删除密钥
func (e *Engine) DeleteKey(key string) error {
	if !e.keyCache.has(key) {
		return ErrKeyNotFound // 避免把"删了个不存在的 key"当成成功
	}
	if err := e.st.DeleteAPIKey(key); err != nil {
		return err
	}
	e.keyCache.del(key)
	return nil
}

// ListKeys 列出全部密钥
func (e *Engine) ListKeys() []*APIKey {
	return e.keyCache.list()
}

// ValidateKey 校验密钥有效（存在 + 未过期）
func (e *Engine) ValidateKey(key string) bool {
	return e.keyCache.valid(key, time.Now().UnixMilli())
}

// ensureDefaultKey 首次启动（无任何 key）自动创建默认永久 key
func (e *Engine) ensureDefaultKey() {
	if len(e.keyCache.list()) > 0 {
		return
	}
	k, err := e.CreateKey("default", 0)
	if err == nil {
		log.Printf("首次启动已自动创建默认 API Key: %s", k.Key)
	}
}

func keyDesc(k *APIKey) string {
	if k.ExpiresAt == 0 {
		return "永不过期"
	}
	return fmt.Sprintf("过期于 %s", time.UnixMilli(k.ExpiresAt).Format("2006-01-02 15:04"))
}

// ---------- Store 层 ----------

const keyPrefixAPIKey = "k:" // k:{key} → JSON

func (s *Store) PutAPIKey(k *APIKey) error {
	data, _ := json.Marshal(k)
	return s.db.Set([]byte(keyPrefixAPIKey+k.Key), data, syncWrite)
}

func (s *Store) ListAPIKeys() []*APIKey {
	var out []*APIKey
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(keyPrefixAPIKey),
		UpperBound: []byte(keyPrefixAPIKey + "\xff"),
	})
	if err != nil {
		return nil
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var k APIKey
		if json.Unmarshal(iter.Value(), &k) == nil {
			out = append(out, &k)
		}
	}
	return out
}

func (s *Store) DeleteAPIKey(key string) error {
	return s.db.Delete([]byte(keyPrefixAPIKey+key), syncWrite)
}
