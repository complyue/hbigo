package main

import (
	"flag"
	"log"
	"net"
	"time"

	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/test/mux"
	"github.com/golang/glog"
)

type Watchable struct {
	hbi.HoContext
}

func (w *Watchable) Watch(interval int) {
	glog.Infof("Events are to be happening at %0.1f Hz.", float64(1000.0/float64(interval)))

	p2p := w.Ho().PoToPeer()
	go func() {
		for {
			if p2p.Cancelled() {
				return
			}

			evt := mux.NewEvt("PubSub style", 5.432)
			glog.V(2).Infof("Happening %#v", evt)
			if err := p2p.NotifBSON(`
Happened()
`, evt, "&Evt{}"); err != nil {
				panic(err)
			}

			time.Sleep(time.Duration(interval) * time.Millisecond)
		}
	}()
}

func (w *Watchable) TypesToExpose() []interface{} {
	return []interface{}{
		(*mux.Evt)(nil),
	}
}

func (w *Watchable) Echo() {
	obj, err := w.Ho().CoRecvObj()
	if err != nil {
		panic(err)
	}
	glog.V(1).Infof("Sending object [%#v] back...\n", obj)
	w.Ho().CoSendBSON(obj, "&Evt{}")
}

func main() {
	flag.Parse()

	hbi.ServeTCP(func() hbi.HoContext {
		return &Watchable{
			HoContext: hbi.NewHoContext(),
		}
	}, ":3232", func(l *net.TCPListener) {
		glog.Infof("test mux server listening %+v", l.Addr())
	})

}

func init() {
	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err := flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %+v", err)
		}
	}
}
