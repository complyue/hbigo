package main

import (
	"flag"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/complyue/hbigo/test/mux"
	"github.com/golang/glog"
)

type Watching struct {
	hbi.HoContext

	cnt            int64
	tsBegin, tsEnd int64
}

func (w *Watching) Happened() {
	evto, err := w.Ho().CoRecvObj()
	if err != nil {
		panic(err)
	}
	evt := evto.(*mux.Evt)
	if w.tsBegin == 0 {
		w.tsBegin = evt.Ts
	}
	w.tsEnd = evt.Ts
	w.cnt++
}

func (w *Watching) TypesToExpose() []interface{} {
	return []interface{}{
		(*mux.Evt)(nil),
	}
}

var interval int64

func main() {
	flag.Int64Var(&interval, "i", 10000, "event interval")
	flag.Parse()

	obj2Echo := mux.NewEvt("RPC style", 2.345)

	w := &Watching{
		HoContext: hbi.NewHoContext(),
	}
	hbic, err := hbi.DialTCP(w, ":3232")
	if err != nil {
		panic(err)
	}

	// ho := hbic.Hosting
	p2p := hbic.Posting

	p2p.Notif(fmt.Sprintf(`
Watch(%d)
`, interval))

	for {

		func() {
			co, err := p2p.Co()
			if err != nil {
				panic(err)
			}
			defer co.Close()

			for i := 0; i < 5; i++ {

				if err := co.SendCode("Echo()"); err != nil {
					panic(err)
				}
				err = co.SendBSON(obj2Echo, "new(Evt)")
				if err != nil {
					panic(err)
				}
				o, err := co.RecvObj()
				if err != nil {
					panic(err)
				}
				if !reflect.DeepEqual(o, obj2Echo) {
					panic(errors.Errorf("Echo mismatch: [%#v] vs [%#v]", o, obj2Echo))
				}

			}
			glog.Info("Echo worked just fine.")

		}()

		glog.Infof("Happened %v times during %d seconds.\n", w.cnt, (w.tsEnd - w.tsBegin))
		time.Sleep(2 * time.Second)
	}

}

func init() {
	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err := flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %+v", err)
		}
	}
}
