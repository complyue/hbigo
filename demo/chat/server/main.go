package main

import (
	"flag"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/demo/chat"
	"github.com/golang/glog"
	"log"
	"net"
)

func init() {
	var err error

	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err = flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %s", err)
		}
	}

}

func main() {

	flag.Parse()

	hbi.ServeTCP(chat.NewChatContext, "127.0.0.1:3232", func(listener *net.TCPListener) {
		log.Println("HBI chat server listening:", listener.Addr())
	})

}
