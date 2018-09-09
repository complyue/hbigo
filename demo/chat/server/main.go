package main

import (
	"flag"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/demo/chat"
)

func main() {

	flag.Parse()

	hbi.ServeTCP(chat.NewChatContext, "127.0.0.1:3232")

}
