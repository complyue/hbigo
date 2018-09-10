package proto

import (
	. "github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
	"github.com/cosmos72/gomacro/fast"
	"reflect"
)

/*
The context for a service hosting endpoint.
*/
type HoContext interface {
	// be a cancellable context
	CancellableContext

	// execute code sent by peer and return last value as result
	Exec(code string) (result interface{}, ok bool, err error)

	// allow manipulation of contextual state objects
	Put(key string, value interface{})
	Get(key string) interface{}

	// the stub if not nil, can be used to post packets/streams to peer
	PoToPeer() Posting
	SetPoToPeer(p2p Posting)

	Close()
}

func NewHoContext() HoContext {
	var ctx = hoContext{
		CancellableContext: NewCancellableContext(),
		interp:             fast.New(),
	}
	return &ctx
}

type hoContext struct {
	// embed a cancellable context
	CancellableContext

	po Posting

	interp *fast.Interp // never change no need to sync
}

func (ctx *hoContext) PoToPeer() Posting {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.po
}

func (ctx *hoContext) SetPoToPeer(p2p Posting) {
	ctx.Lock()
	defer ctx.Unlock()
	ctx.po = p2p
}

func (ctx *hoContext) Cancel(err error) {
	// make sure posting context cancelled as well
	if p2p := ctx.PoToPeer(); p2p != nil {
		ctx.SetPoToPeer(nil)
		p2p.Cancel(err)
	}
	ctx.CancellableContext.Cancel(err)
}

func (ctx *hoContext) Close() {
	// make sure posting context closed with hosting context
	if p2p := ctx.PoToPeer(); p2p != nil {
		ctx.SetPoToPeer(nil)
		p2p.Close()
	}
	// make sure done channel closed
	ctx.Cancel(nil)
}

func (ctx *hoContext) Exec(code string) (result interface{}, ok bool, err error) {
	defer func() {
		// gomacro Eval may panic, convert it to returned error here
		if e := recover(); e != nil {
			ok = false
			err = RichError(e)
		}
	}()

	// no lock as meant to be called only from landing goro
	rvs, _ := ctx.interp.Eval(code)
	switch len(rvs) {
	case 0:
		// void value, leave result be nil, ok be false
	case 1:
		// single value landed
		r := rvs[0].Interface()
		result, ok = r, true
	default:
		// multiple values landed
		r := make([]interface{}, len(rvs))
		for _, v := range rvs {
			r = append(r, v.Interface())
		}
		result, ok = r, true
	}
	return
}

func (ctx *hoContext) Get(key string) interface{} {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.interp.ValueOf(key).Interface()
}

func (ctx *hoContext) Put(key string, value interface{}) {
	ctx.Lock()
	defer ctx.Unlock()
	ctx.put(key, value)
}

func (ctx *hoContext) put(key string, value interface{}) {
	interp := ctx.interp
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
		interp.DeclFunc(key, v.Interface())
	default:
		interp.DeclVar(key, nil, v.Interface())
	}
}

// string keys are not recommended for context.Context,
// while we provide our contextual state object keyed by string
func (ctx *hoContext) Value(key interface{}) interface{} {
	if sk, ok := key.(string); ok {
		return ctx.Get(sk)
	}
	return nil
}
