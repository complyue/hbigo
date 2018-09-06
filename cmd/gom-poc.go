package main

import (
	"github.com/cosmos72/gomacro/fast"
	"log"
	"os"
)

func main() {
	var err error

	log.Println("GP:", os.Getenv("GOPATH"))

	lastMsg := ""
	interp := fast.New()
	interp.DeclFunc("ppp", func(msg string) string {
		if len(msg) > 0 {
			lastMsg = msg
		}
		return lastMsg
	})
	msg, typ := interp.Eval(`
msg := "xxx"
ppp(msg)
`)
	log.Printf("Got msg %v of %v", msg, typ)
	if nil != err {
		log.Fatal("Error: ", err)
		return
	}

	lastMsg = "qqq"

	msg, typ = interp.Eval(`
ppp("")
`)
	log.Printf("Got msg %v of %v", msg, typ)
	if nil != err {
		log.Fatal("Error: ", err)
		return
	}

	log.Println("Done.")
}
