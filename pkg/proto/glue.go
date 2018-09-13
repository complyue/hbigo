package proto

import (
	"bytes"
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
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

	// add some overridable utility funcs
	exports["NewError"] = errors.New

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
		for fi, nf := 0, ct.NumField(); fi < nf; fi++ {
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

	var apiText bytes.Buffer
	for k, v := range exports {
		if _, ok := expBlackList[k]; ok {
			// in black list, skip
			continue
		}
		hc.put(k, v)
		apiText.WriteString(fmt.Sprintf(" * func - %s:\n\t%#v\n", k, v))
	}
	hc.put("API", apiText.String())

	// plant some non-overridable utility funcs
	hc.put("pong", func() {}) // nop react to ping back
	hc.put("ping", func() {   // react to connectivity test, pong back
		if po := hc.po; po != nil {
			po.Notif("pong()")
		}
	})
	// expose methods to access the hosting object
	hc.put("Ho", hc.Ho)
	hc.put("PoToPeer", hc.PoToPeer)
}

var expBlackList map[string]struct{}
