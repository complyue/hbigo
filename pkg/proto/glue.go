package proto

import (
	"bytes"
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"reflect"
)

/*
Properly export contextual artifacts (fields, methods, types, etc.) for hosting.
*/
func PrepareHosting(ctx HoContext) {
	// not to be a method of hoContext, or can't see the whole struct
	if ctx == nil {
		panic(errors.NewUsageError("nil ctx?!"))
	}
	var (
		hc              *hoContext
		noCustomization bool
		pv              = reflect.ValueOf(ctx)
		pt              = pv.Type()
		cv              = pv.Elem()
		ct              = cv.Type()
		exports         = make(map[string]interface{})
	)
	if hc, noCustomization = ctx.(*hoContext); !noCustomization {
		// collect exported fields of the context struct, and extract embedded hoContext btw
		for fi, nf := 0, ct.NumField(); fi < nf; fi++ {
			sf := ct.Field(fi)
			fv := cv.Field(fi)
			if sf.PkgPath != "" {
				continue // ignore unexported field
			}
			if sf.Anonymous {
				if "HoContext" == sf.Name {
					// there should be one and only one embedded anonymous HoContext
					hc = fv.Interface().(*hoContext)
				}
				continue
			}
			// expose field getter/setter func
			exports[sf.Name] = func() interface{} {
				return fv.Interface()
			}
			exports["Set"+sf.Name] = reflect.MakeFunc(
				reflect.FuncOf([]reflect.Type{sf.Type}, []reflect.Type{}, false),
				func(args []reflect.Value) (results []reflect.Value) {
					fv.Set(args[0])
					return
				},
			)
		}
	}
	if hc == nil {
		panic(errors.NewUsageError(fmt.Sprintf("No embedded HoContext in struct %s ?!", ct.Name())))
	}

	if !noCustomization {
		// collected exported methods of the context struct
		for mi, nm := 0, pv.NumMethod(); mi < nm; mi++ {
			mt := pt.Method(mi)
			if mt.PkgPath != "" {
				continue // ignore unexported method
			}
			mv := pv.Method(mi)
			exports[mt.Name] = mv
		}
	}

	// prepare black list for names to be filtered when planting artifacts into interpreter,
	// i.e. all fields/methods promoted from embedded HoContext should be excluded, except the
	// explicitly exposed utility methods
	if expBlackList == nil {
		expBlackList = make(map[string]struct{}, len(exports))
		pt := reflect.ValueOf(hc).Type()
		vt := pt.Elem()
		for fi, nf := 0, vt.NumField(); fi < nf; fi++ {
			sf := vt.Field(fi)
			if sf.PkgPath != "" {
				continue // ignore unexported field
			}
			expBlackList[sf.Name] = struct{}{}
		}
		for mi, nm := 0, pt.NumMethod(); mi < nm; mi++ {
			mt := pt.Method(mi)
			if mt.PkgPath != "" {
				continue // ignore unexported method
			}
			expBlackList[mt.Name] = struct{}{}
		}
	}

	// plant collected exports into interpreter
	hc.Lock()
	defer hc.Unlock()

	// expose types requested by the ctx
	for _, v := range ctx.TypesToExpose() {
		vt := reflect.TypeOf(v)
		t := vt
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		hc.put("", t)
	}

	var apiText bytes.Buffer
	for k, v := range exports {
		if _, ok := expBlackList[k]; ok {
			// in black list, skip
			continue
		}
		hc.put(k, v)
		apiText.WriteString(fmt.Sprintf(" * func - %s:\n\t%#v\n", k, v))
	}
	// poorman's API manual
	hc.put("API", apiText.String())

	// plant some non-overridable utility funcs

	// ping/pong game for alive-checking/keeping
	hc.put("pong", func() {}) // nop response to ping by remote
	// react to connectivity test, send pong() back
	hc.put("ping", func() {
		if po := hc.po; po != nil {
			po.Notif("pong()")
		}
	})

	// only the packet landing goroutine should eval against interpreter, it is
	// bad for other goros of non-interpreted code to call interpreter's eval.
	// so as part of the corun landing procedure, a new goro has to be started
	// by eval `go corun(...)` against interpreter from the landing loop.
	hc.put("corun", func(coro func()) {
		ho := hc.Ho().(*HostingEndpoint)

		func() { // check co id
			ho.muHo.Lock()
			defer ho.muHo.Unlock()

			if ho.coId == "" {
				panic(errors.Errorf("corun without co id?"))
			}
		}()

		coro() // this is interpreter parsed code to be co-run

		func() { // clean co id
			ho.muHo.Lock()
			defer ho.muHo.Unlock()

			if ho.coId == AdhocCoId { // close the conversation if it is ad-hoc
				ho.coId = ""
			}
		}()

	})

	// expose the bson receiver method, converting err-out to panic.
	// note `(Co)SendBSON()` depends on availability of this method
	// at peer hosting env to work
	hc.put("recvBSON", func(nBytes int, booter interface{}) interface{} {
		ho := hc.Ho().(*HostingEndpoint)
		if ho.coId == "" {
			panic(errors.NewUsageError("recvBSON called without conversation ?!"))
		}
		o, err := ho.recvBSON(nBytes, booter)
		if err != nil {
			glog.Error(errors.RichError(err))
			panic(err)
		}
		return o
	})

	// expose methods for hosted error (re)construction
	exports["NewError"] = errors.New

	// plant some common funcs for diagnostics

	// expose methods to access the hosting object
	hc.put("Ho", hc.Ho)
	// expose methods to access the posting object
	hc.put("PoToPeer", hc.PoToPeer)

	// expose methods to reveal landing result by this hosting context to peer context
	// todo use something better than println() ?
	hc.put("reveal", func(format string, a ...interface{}) {
		s := fmt.Sprintf(format, a...)
		if err := hc.PoToPeer().Notif(fmt.Sprintf(`
println(%#v)
`, s)); err != nil {
			panic(err)
		}
	})

}

var expBlackList map[string]struct{}
