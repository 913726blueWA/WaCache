package Cache

import (
	"container/list"
)

type Cache interface {
	Get(key string) (Value, bool)
	Add(key string, value Value)
	Delete(key string) bool
}

// Value use Len to count how many bytes it takes
type Value interface {
	Len() int
}

// a LRU Cache. It is not safe for concurrent access.
type lruCache struct {
	maxBytes  int64
	usedBytes int64
	ll        *list.List
	cache     map[string]*list.Element
	OnEvicted func(key string, value Value) // optional and executed when an entry is purged.
}

type entry struct {
	key   string
	value Value
}

// NewLRUCache is the Constructor of lruCache
func NewLRUCache(maxBytes int64, onEvicted func(string, Value)) *lruCache {
	return &lruCache{
		maxBytes:  maxBytes,
		usedBytes: 0,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// Get look ups a key's value
func (c *lruCache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return nil, false
}

// Add a value to the cache.
func (c *lruCache) Add(key string, value Value) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.usedBytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.usedBytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.maxBytes < c.usedBytes {
		c.RemoveOldest()
	}
}

func (c *lruCache) Delete(key string) bool {
	c.usedBytes -= int64(len(key) + c.getValueSizeByKey(key))
	delete(c.cache, key)
	return true
}

// RemoveOldest removes the oldest item
func (c *lruCache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.usedBytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Len the number of cache entries
func (c *lruCache) Len() int {
	return c.ll.Len()
}
func (c *lruCache) getValueSizeByKey(key string) int {
	return c.cache[key].Value.(*entry).value.Len()
}
