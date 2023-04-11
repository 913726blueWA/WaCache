package waCache

import (
	Cache "WaCache/cache"
	"sync"
)

// 实现缓存并发特性
type cache struct {
	mu         sync.RWMutex
	lru        Cache.Cache
	cacheBytes int64
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = Cache.NewLRUCache(c.cacheBytes, nil)
	}
	c.lru.Add(key, value)
}

func (c *cache) get(key string) (byteView ByteView, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.lru == nil {
		return
	}

	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), ok
	}
	return
}
func (c *cache) delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return true
	}
	return c.lru.Delete(key)
}
