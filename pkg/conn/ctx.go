package conn

import (
	"context"
	"fmt"
	"github.com/cosmos72/gomacro/fast"
	"go/types"
	"reflect"
	"sync/atomic"
	"time"
)

type Context interface {
	context.Context
	Cancel(err error)
	Cancelled() bool

	Peer() Connection

	Get(key string) interface{}
	Put(key string, value interface{})
}

func NewContext(populations ...interface{}) Context {
	var hbic hbiContext
	hbic.done = make(chan struct{})
	hbic.interp = fast.New()

	for _, popu := range populations {
		var coll reflect.Value
		switch somebody := popu.(type) {
		case reflect.Type:
			hbic.Put(somebody.Name(), somebody)
			continue
		case types.Func:
			hbic.Put(somebody.Name(), somebody)
			continue
		case reflect.Value:
			coll = somebody
		default:
			coll = reflect.ValueOf(popu)
		}
		switch coll.Kind() {
		case reflect.Struct:
			t := coll.Type()
			for i, n := 0, t.NumField(); i < n; i++ {
				fn := t.Field(i).Name
				fv := coll.Field(i)
				hbic.Put(fn, fv)
			}
		case reflect.Map:
			for _, key := range coll.MapKeys() {
				mk := fmt.Sprintf("%s", key)
				mv := coll.MapIndex(key).Interface()
				hbic.Put(mk, mv)
			}
		default:
			panic(fmt.Sprintf("Unsupported population type: %t", popu))
		}
	}

	return &hbic
}

type hbiContext struct {
	done   chan struct{} // todo use atomic.Value too ??
	err    atomic.Value
	peer   Connection // todo use atomic.Value too ??
	interp *fast.Interp
}

func (hbic *hbiContext) Deadline() (deadline time.Time, ok bool) {
	// never has a deadline
	return
}

func (hbic *hbiContext) Done() <-chan struct{} {
	return hbic.done
}

func (hbic *hbiContext) Err() error {
	return hbic.err.Load().(error)
}

func (hbic *hbiContext) Value(key interface{}) interface{} {
	if sk, ok := key.(string); ok {
		return hbic.Get(sk)
	}
	return nil
}

// since there's no way to check whether a chan is closed without blocking,
// and once cancelled, Context.Done() should never block, so cancellation
// is implemented by assigning hbiContext::done to this closed chan as a marker
var closedChan = make(chan struct{})

func init() {
	close(closedChan)
}

func (hbic *hbiContext) Cancel(err error) {
	if err != nil {
		hbic.err.Store(err)
	}
	if done := hbic.done; done != closedChan {
		hbic.done = closedChan
		close(done)
	}
}

func (hbic *hbiContext) Cancelled() bool {
	return hbic.done == closedChan
}

func (hbic *hbiContext) Peer() Connection {
	return hbic.peer
}

func (hbic *hbiContext) String() string {
	// todo add meaningful artifacts
	return fmt.Sprintf("hbi.Context@%p", hbic)
}

func (hbic *hbiContext) Get(key string) interface{} {
	return hbic.interp.ValueOf(key).Interface()
}

func (hbic *hbiContext) Put(key string, value interface{}) {
	interp := hbic.interp
	if t, ok := value.(reflect.Type); ok {
		interp.DeclTypeAlias(key, interp.Comp.Universe.FromReflectType(t))
		return
	}
	v, ok := value.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(value)
	}
	switch v.Kind() {
	case reflect.Func:
		interp.DeclFunc(key, v)
	default:
		interp.DeclVar(key, nil, v)
	}
}
