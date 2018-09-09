package main

import (
	"flag"
	"github.com/complyue/hbigo"
	"github.com/peterh/liner"
	"log"
)

func main() {

	flag.Parse()

	hbic, err := hbi.DialTCP(hbi.NewHoContext(), "localhost:3232")
	if err != nil {
		log.Fatal(err)
		return
	}

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	for {

		if hbic.Posting.Cancelled() {
			break
		}

		code, err := line.Prompt("hbi chat> ")
		if err != nil {
			log.Fatal(err)
			return
		}

		hbic.Notif(code)
	}

	log.Printf("Done.")

}
