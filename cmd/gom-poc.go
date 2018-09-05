package main

import (
	"github.com/complyue/hbigo/pkg/echo"
	"github.com/cosmos72/gomacro/fast"
	"log"
	"os"
)

func main() {
	var err error

	log.Println("GP:", os.Getenv("GOPATH"))

	interp := fast.New()
	interp.DeclFunc("ppp", echo.Ppp)
	msg, typ := interp.Eval(`
msg := "xxx"
ppp(msg)
`)
	log.Printf("Got msg %v of %v", msg, typ)
	if nil != err {
		log.Fatal("Error: ", err)
		return
	}

	log.Println("Done.")
}
