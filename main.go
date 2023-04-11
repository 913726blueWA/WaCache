package main

import (
	"WaCache/waCache"
	"errors"
	"flag"
	"log"
	"strconv"
)

func main() {
	var port int
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.Parse()
	// mock database or other dataSource
	var mysql = map[string]string{
		"Tom":  "630",
		"Tom1": "631",
		"Tom2": "632",
	}
	// NewGroup create a Group which means a kind of sources
	// contain a func that used when misses cache
	g := waCache.NewGroup("scores", 2<<10, waCache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := mysql[key]; ok {
				return []byte(v), nil
			}
			return nil, errors.New("not hit")
		}))

	var addr string = "127.0.0.1:" + strconv.Itoa(port)

	server, err := waCache.NewServer(addr)
	if err != nil {
		log.Fatal(err)
	}

	picker := waCache.NewClientPicker(addr)
	g.RegisterPeers(picker)

	for {
		err = server.Start()
		if err != nil {
			log.Println(err.Error())
			return
		}
	}
}
