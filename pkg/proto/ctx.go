package proto

import (
	"github.com/complyue/goScript"
	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
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

	// allow manipulation of contextual state objects
	Put(key string, value interface{})
	Get(key string) interface{}

	// the hosting endpoint embedding this HoContext
	Ho() Hosting
	SetHo(ho Hosting)

	// the posting endpoint bound to this context
	// can be nil for a receive only setup
	PoToPeer() Posting
	SetPoToPeer(po Posting)
	// panic instead of returning nil if no posting endpoint available
	MustPoToPeer() Posting

	Close()
}

func NewHoContext() HoContext {
	var ctx = hoContext{
		CancellableContext: NewCancellableContext(),
		env:                make(map[string]interface{}, 50),
	}
	return &ctx
}

type hoContext struct {
	// embed a cancellable context
	CancellableContext

	ho Hosting
	po Posting

	env map[string]interface{}
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

func (ctx *hoContext) MustPoToPeer() Posting {
	po := ctx.PoToPeer()
	if po == nil { // must be available, or panic
		var err error
		if ctx.Cancelled() {
			// propagate previous error
			err = ctx.Err()
		}
		if err == nil {
			err = errors.NewUsageError("No posting endpoint available.")
		}
		if !ctx.Cancelled() {
			ctx.Cancel(err)
		}
		panic(err)
	}
	if po.Cancelled() {
		err := po.Err()
		if err == nil {
			err = errors.NewUsageError("Posting endpoint disconnected.")
		}
		panic(err)
	}
	return po
}

func (ctx *hoContext) PoToPeer() Posting {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.po
}

func (ctx *hoContext) SetPoToPeer(po Posting) {
	ctx.Lock()
	defer ctx.Unlock()
	ctx.po = po
}

func (ctx *hoContext) Cancel(err error) {
	if ctx.CancellableContext.Cancelled() {
		return
	}
	ctx.CancellableContext.Cancel(err)
	po := ctx.PoToPeer()
	if po != nil && !po.Cancelled() {
		po.Cancel(err)
	}
	ho := ctx.Ho()
	if ho != nil && !ho.Cancelled() {
		ho.Cancel(err)
	}
}

func (ctx *hoContext) Close() {
	// currently implemented by cancelling with nil error
	ctx.Cancel(nil)
}

// calls to this method must be properly sync'ed
func (ctx *hoContext) Exec(code string) (result interface{}, ok bool, err error) {
	result, err = goScript.RunInContext("HBI-CODE", code, ctx.env)
	if err != nil {
		return nil, false, err
	}
	ok = true
	return
}

func (ctx *hoContext) Get(key string) interface{} {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.env[key]
}

func (ctx *hoContext) Put(key string, value interface{}) {
	ctx.Lock()
	defer ctx.Unlock()
	ctx.env[key] = value
}

// string keys are not recommended for context.Context,
// while we provide our contextual state object keyed by string
func (ctx *hoContext) Value(key interface{}) interface{} {
	if sk, ok := key.(string); ok {
		return ctx.Get(sk)
	}
	return nil
}
