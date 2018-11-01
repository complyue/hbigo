package proto

import (
	"fmt"
	"reflect"

	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
	"github.com/cosmos72/gomacro/fast"
)

/*
The context for a service hosting endpoint.

An HBI gate struct will at least embed an interface of this type constructed by hbi.NewHoContext(),
with service methods (and optionally some fields for service gate contextual states) defined
to the struct in addition.

These methods (and setter/getter of fields) will be available (i.e. exposed) to HBI connection peers
for scripted conversations & notifications.

Methods (include getters/setters) of this interface can be intercepted (overridden in Go's ways) by
the HBI gate struct purposefully.

*/
type HoContext interface {
	// be a cancellable context
	CancellableContext

	// return a slice of values, normally typed nil pointers or zero values,
	// whose value types are to be exposed to the hosting environment.
	TypesToExpose() []interface{}

	// execute code sent by peer and return last value as result
	Exec(code string) (result interface{}, ok bool, err error)
	// Start a new goroutine from the interpreter, running specified code
	GoExec(code string) (err error)

	// allow manipulation of contextual state objects
	Put(key string, value interface{})
	Get(key string) interface{}

	// the hosting endpoint embedding this HoContext
	Ho() Hosting
	SetHo(ho Hosting)

	// the posting endpoint bound to this context
	// can be nil for a receive only setup
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

	ho Hosting
	po Posting

	interp *fast.Interp // never change no need to sync
}

func (ctx *hoContext) TypesToExpose() []interface{} {
	return []interface{}{}
}

func (ctx *hoContext) Ho() Hosting {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.ho
}

func (ctx *hoContext) SetHo(ho Hosting) {
	ctx.Lock()
	defer ctx.Unlock()
	ctx.ho = ho
}

func (ctx *hoContext) PoToPeer() Posting {
	ctx.RLock()
	defer ctx.RUnlock()
	if ctx.po == nil {
		if ctx.Cancelled() {
			err := ctx.Err()
			if err != nil {
				// propagate connection errors
				panic(err)
			}
		}
		panic(errors.NewUsageError("No posting endpoint available."))
	}
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

// calls to this method must be properly sync'ed against the interpreter
func (ctx *hoContext) Exec(code string) (result interface{}, ok bool, err error) {
	defer func() {
		// gomacro Eval may panic, convert it to returned error here
		if e := recover(); e != nil {
			ok = false
			err = errors.Wrapf(errors.RichError(e), "Error landing exec code: \n%s\n", code)
		}
	}()

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

// calls to this method must be properly sync'ed against the interpreter
func (ctx *hoContext) GoExec(code string) (err error) {
	defer func() {
		// gomacro Eval may panic, convert it to returned error here
		if e := recover(); e != nil {
			err = errors.Wrapf(errors.RichError(e), "Error landing corun code: \n%s\n", code)
		}
	}()

	rvs, _ := ctx.interp.Eval(fmt.Sprintf(`
go func() {

%s

}()
`, code))
	if len(rvs) != 0 {
		err = errors.Errorf("Unexpected result from GoExec: %+v\n", rvs)
		return
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
		if key == "" {
			key = t.Name()
		}
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
