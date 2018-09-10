package main

import (
	"flag"
	"github.com/complyue/hbigo"
	"github.com/golang/glog"
	"github.com/peterh/liner"
	"log"
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
