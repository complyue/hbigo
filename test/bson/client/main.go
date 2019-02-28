package main

import (
	"flag"
	"log"
	"reflect"

	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/glog"
)

type Watching struct {
	hbi.HoContext
}

func (w *Watching) TypesToExpose() []interface{} {
	return []interface{}{
		(bson.M)(nil),
	}
}

var interval int64

func main() {
	flag.Parse()

	obj2Echo := bson.M{
		"RPC style": 2.345,
	}

	w := &Watching{
		HoContext: hbi.NewHoContext(),
	}
	hbic, err := hbi.DialTCP(w, ":3232")
	if err != nil {
		panic(err)
	}
	defer hbic.Close()

	// ho := hbic.Hosting
	p2p := hbic.Posting

	func() {
		co, err := p2p.Co()
		if err != nil {
			panic(err)
		}
		defer co.Close()

		for i := 0; i < 5; i++ {
			obj2Echo["n"] = i
			obj2Echo["id"] = bson.NewObjectId()

			if err := co.SendCode("Echo()"); err != nil {
				panic(err)
			}
			err = co.SendBSON(obj2Echo, "")
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

}

func init() {
	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err := flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %+v", err)
		}
	}
}
