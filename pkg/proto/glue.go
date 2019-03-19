package proto

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
)

// scan for the deepest *hoContext embedded into the interface,
// collect exported methods of root, and getters/setters for exported fields at any level.
func scanForExports(ctx HoContext, exports map[string]interface{}, root *hoContext) *hoContext {
	pv := reflect.ValueOf(ctx)
	pt := pv.Type()
	if pt.Kind() != reflect.Ptr {
		panic("HoContext need pointer implementation, struct implementation not expected!")
	}
	cv := pv.Elem()
	ct := cv.Type()

	if root == nil {
		root, _ = ctx.(*hoContext)
	}
	// collect exported fields of the context struct
	for fi, nf := 0, ct.NumField(); fi < nf; fi++ {
		sf := ct.Field(fi)
		fv := cv.Field(fi)
		if sf.PkgPath != "" {
			continue // ignore unexported field
		}
		if sf.Anonymous {
			if "HoContext" == sf.Name {
				// scan embedded HoContext recursively until found the deepest
				if root == nil {
					root = scanForExports(fv.Interface().(HoContext), exports, root)
				}
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
		).Interface()
	}
	if ctx != root {
		// collect exported methods of the context struct
		for mi, nm := 0, pv.NumMethod(); mi < nm; mi++ {
			mt := pt.Method(mi)
			if mt.PkgPath != "" {
				continue // ignore unexported method
			}
			mv := pv.Method(mi)
			exports[mt.Name] = mv.Interface()
		}
	}
	return root
}

/*
Properly export contextual artifacts (fields, methods, types, etc.) for hosting.
*/
func PrepareHosting(ctx HoContext) {
	// not to be a method of hoContext, or can't see the whole struct
	if ctx == nil {
		panic(errors.NewUsageError("nil ctx?!"))
	}
	var (
		hc      *hoContext
		exports = make(map[string]interface{})
	)
	hc = scanForExports(ctx, exports, nil)
	if hc == nil {
		panic(errors.NewUsageError(fmt.Sprintf("No embedded HoContext in struct %T ?!", ctx)))
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
		hc.env[t.Name()] = t
	}

	var apiText bytes.Buffer
	for k, v := range exports {
		if _, ok := expBlackList[k]; ok {
			// in black list, skip
			continue
		}
		hc.env[k] = v
		apiText.WriteString(fmt.Sprintf(" * func - %s:\n\t%#v\n", k, v))
	}
	// poorman's API manual
	hc.env["API"] = apiText.String()

	// plant some non-overridable utility funcs

	// todo: implement this by goScript in a better way
	hc.env["new"] = func(rt reflect.Type) interface{} {
		return reflect.New(rt).Interface()
	}

	// ping/pong game for alive-checking/keeping
	hc.env["pong"] = func() {} // nop response to ping by remote
	// react to connectivity test, send pong() back
	hc.env["ping"] = func() {
		if po := hc.po; po != nil {
			po.Notif("pong()")
		}
	}

	// expose the bson receiver method, converting err-out to panic.
	// note `(Co)SendBSON()` depends on availability of this method
	// at peer hosting env to work
	hc.env["recvBSON"] = func(nBytes int, booter interface{}) interface{} {
		ho := hc.Ho().(*HostingEndpoint)
		o, err := ho.recvBSON(nBytes, booter)
		if err != nil {
			glog.Error(errors.RichError(err))
			panic(errors.RichError(err))
		}
		return o
	}

	// expose methods for hosted error (re)construction
	exports["NewError"] = errors.New

	// plant some common funcs for diagnostics

	// expose methods to access the hosting object
	hc.env["Ho"] = hc.Ho
	// expose methods to access the posting object
	hc.env["PoToPeer"] = hc.PoToPeer

	// expose methods to reveal landing result by this hosting context to peer context
	// todo use something better than println() ?
	hc.env["reveal"] = func(format string, a ...interface{}) {
		s := fmt.Sprintf(format, a...)
		if err := hc.PoToPeer().Notif(fmt.Sprintf(`
println(%#v)
`, s)); err != nil {
			panic(errors.RichError(err))
		}
	}

}

var expBlackList map[string]struct{}
