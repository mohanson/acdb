package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/mohanson/acdb"
	"github.com/mohanson/doa"
)

var (
	flListen = flag.String("l", "127.0.0.1:8080", "listen address")
	flRoot   = flag.String("d", ".", "root directory")
	client   acdb.Client
)

func hand(w http.ResponseWriter, r *http.Request) {
	k := r.URL.EscapedPath()
	if k == "/" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		b, err := client.Get(k)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			return
		}
		w.Write(b)
	case http.MethodPut:
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			return
		}
		log.Println("set", k, string(b))
		if err := client.Set(k, b); err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			return
		}
	case http.MethodDelete:
		log.Println("del", k)
		if err := client.Del(k); err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			return
		}
	}
}

func main() {
	flag.Parse()
	client = acdb.Map(*flRoot)
	http.HandleFunc("/", hand)
	doa.Try1(http.ListenAndServe(*flListen, nil))
}
