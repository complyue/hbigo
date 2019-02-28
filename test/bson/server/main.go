package main

import (
	"flag"
	"log"
	"net"

	"github.com/complyue/hbigo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/glog"
)

type Wall struct {
	hbi.HoContext
}

func (w *Wall) TypesToExpose() []interface{} {
	return []interface{}{
		(bson.M)(nil),
	}
}

func (w *Wall) Echo() {
	obj, err := w.Ho().CoRecvObj()
	if err != nil {
		panic(err)
	}
	glog.V(1).Infof("Sending object [%#v] back...\n", obj)
	w.Ho().CoSendBSON(obj, "")
}

func main() {
	flag.Parse()

	hbi.ServeTCP(func() hbi.HoContext {
		return &Wall{
			HoContext: hbi.NewHoContext(),
		}
	}, ":3232", func(l *net.TCPListener) {
		glog.Infof("test bson server listening %+v", l.Addr())
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
