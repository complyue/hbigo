package proto

import (
	"fmt"
	. "github.com/complyue/hbigo/pkg/errors"
	"reflect"
)

/*
Properly export contextual artifacts (fields, methods, types, etc.) for hosting.
*/
func PrepareHosting(ctx HoContext) {
	// not to be a method of hoContext, or can't see the whole struct
	if ctx == nil {
		panic(UsageError{"nil ctx?!"})
	}
	if _, ok := ctx.(*hoContext); ok {
		// shortcut if ctx is a direct hoContext, i.e. not embedding
		return
	}
	var (
		cv      = reflect.ValueOf(ctx).Elem() // must be a pointer type
		ct      = cv.Type()
		nf      = ct.NumField()
		hc      *hoContext
		exports = make(map[string]interface{})
	)
	// collect exported fields of the context struct, and extract embedded hoContext btw
	for fi := 0; fi < nf; fi++ {
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
		// expose a field getter func
		exports[sf.Name] = func() interface{} {
			return fv.Interface()
		}
	}
	if hc == nil {
		panic(UsageError{fmt.Sprintf("No embedded HoContext in struct %s ?!", ct.Name())})
	}
	// collected exported methods of the context struct
	for mi, nm := 0, ct.NumMethod(); mi < nm; mi++ {
		mt := ct.Method(mi)
		if mt.PkgPath != "" {
			continue // ignore unexported method
		}
		mv := cv.Method(mi)
		exports[mt.Name] = mv
	}

	// plant collected exports into interpreter
	hc.Lock()
	defer hc.Unlock()
	for k, v := range exports {
		hc.put(k, v)
	}
}
