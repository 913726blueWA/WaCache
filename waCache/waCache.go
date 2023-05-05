package waCache

import (
	"WaCache/singleflight"
	"fmt"
	"log"
	"sync"
)

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// Getter 回调Getter,缓存不存在时,调用获取源数据
type Getter interface {
	Get(key string) ([]byte, error)
}

type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// Group 是一个缓存命名空间,负责与用户的交互，并且控制缓存值存储和获取的流程。
type Group struct {
	name      string
	getter    Getter
	mainCache cache
	peers     PeerPicker
	loader    *singleflight.Group
}

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	mu.Lock()
	defer mu.Unlock()
	groups[name] = g
	return g
}

func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] ", key, " hit")
		return v, nil
	}
	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	view, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok, isSelf := g.peers.PickPeer(key); ok {
				if !isSelf {
					if value, err := g.getFromPeer(peer, key); err == nil {
						return value, nil
					} else {
						log.Println("[Geek-Cache] Failed to get from peer", err)
					}
				}
			}
		}
		return g.getLocally(key)
	})

	if err == nil {
		return view.(ByteView), nil
	}
	return ByteView{}, err
}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{
		bytes: bytes,
	}, nil
}

func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{bytes: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

func (g *Group) Delete(key string) (bool, error) {
	if key == "" {
		return true, fmt.Errorf("key is required")
	}
	// Peer is not set, delete from local
	if g.peers == nil {
		return g.mainCache.delete(key), nil
	}
	// The peer is set,
	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok {
		return false, nil
	}
	if isSelf {
		return g.mainCache.delete(key), nil
	} else {
		//use other server to delete the key-value
		success, err := g.deleteFromPeer(peer, key)
		return success, err
	}
}
func (g *Group) deleteFromPeer(peer PeerGetter, key string) (bool, error) {
	success, err := peer.Delete(g.name, key)
	if err != nil {
		return false, err
	}
	return success, nil
}
