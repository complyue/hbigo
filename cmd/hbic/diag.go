package main

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/proto"
	"github.com/golang/glog"
	"os"
)

func NewDiagnosticContext() *DiagnosticContext {
	return &DiagnosticContext{
		HoContext:   hbi.NewHoContext(),
		InbCodeHist: []string{},
		chResult:    make(chan interface{}),
		chVoid:      make(chan struct{}),
		chErr:       make(chan error),
		chCo:        make(chan struct{}),
	}
}

type DiagnosticContext struct {
	hbi.HoContext

	// the active delegate
	Delegate hbi.HoContext

	// history of inbound code
	InbCodeHist     []string
	NextCodeToLand  int
	InbCoRunHist    []string
	NextCoRunToLand int

	// history of landed result
	LandHist []interface{}

	// channel to send outbound objects
	chResult chan interface{}
	chVoid   chan struct{}
	chErr    chan error
	chCo     chan struct{}
}

func (dc *DiagnosticContext) SetDelegate(dele hbi.HoContext) {
	if dele != nil {
		proto.PrepareHosting(dele)
		dele.SetHo(dc.Ho())
		dele.SetPoToPeer(dc.PoToPeer())
	}
	dc.Delegate = dele
	if dele == nil {
		return
	}
	for dc.NextCodeToLand < len(dc.InbCodeHist) {
		result, _, err := dele.Exec(dc.InbCodeHist[dc.NextCodeToLand])
		if err != nil {
			glog.Error(err)
			return
		}
		dc.NextCodeToLand++
		dc.LandHist = append(dc.LandHist, result)
	}
}

// hijack code landing logic
func (dc *DiagnosticContext) Exec(code string) (result interface{}, ok bool, err error) {
	dc.InbCodeHist = append(dc.InbCodeHist, code)

	if dc.NextCodeToLand == len(dc.InbCodeHist)-1 && dc.Delegate != nil {
		// fluent landing by delegate, continue it
		result, ok, err = dc.Delegate.Exec(code)
		if err != nil {
			glog.Error(err)
			return
		}
		dc.NextCodeToLand++
		dc.LandHist = append(dc.LandHist, result)
		return
	}

	fmt.Fprintf(os.Stderr, "CodeIn[%d]:\n#-#-#\n%s\n*-*-*\n", len(dc.InbCodeHist), code)

	select {
	case err = <-dc.chErr:
		return
	case result = <-dc.chResult:
	case <-dc.chVoid:
		ok = false
	}

	dc.NextCodeToLand++
	dc.LandHist = append(dc.LandHist, result)

	return
}

// hijack code landing logic
func (dc *DiagnosticContext) CoExec(code string) (err error) {
	dc.InbCoRunHist = append(dc.InbCoRunHist, code)

	if dc.NextCoRunToLand == len(dc.InbCoRunHist)-1 && dc.Delegate != nil {
		// fluent landing by delegate, continue it
		err = dc.Delegate.CoExec(code)
		if err != nil {
			glog.Error(err)
			return
		}
		dc.NextCoRunToLand++
		return
	}

	fmt.Fprintf(os.Stderr, "CoRunIn[%d]:\n#-#-#\n%s\n*-*-*\n", len(dc.InbCoRunHist), code)

	select {
	case err = <-dc.chErr:
		return
	case <-dc.chCo:
	}

	dc.NextCoRunToLand++

	return
}

func (dc *DiagnosticContext) LandOne() (result interface{}, ok bool, err error) {
	if dc.NextCodeToLand >= len(dc.InbCodeHist) {
		fmt.Fprintf(os.Stderr, "[diag] no code pending.")
		return
	}

	code := dc.InbCodeHist[dc.NextCodeToLand]
	if dc.Delegate != nil {
		result, ok, err = dc.Delegate.Exec(code)
	} else {
		result, ok, err = dc.HoContext.Exec(code)
	}
	if err != nil {
		glog.Error(err)
		return
	}
	dc.NextCodeToLand++

	return
}

func (dc *DiagnosticContext) AutoVoid() {
	close(dc.chVoid)
}

func (dc *DiagnosticContext) ManualVoid() {
	dc.chVoid = make(chan struct{})
}

func (dc *DiagnosticContext) Fake(result interface{}) {
	select {
	case dc.chResult <- result:
	default:
		fmt.Fprintln(os.Stderr, "[diag] no code pending.")
	}
}

func (dc *DiagnosticContext) FakeVoid() {
	select {
	case dc.chVoid <- struct{}{}:
	default:
		fmt.Fprintln(os.Stderr, "[diag] no code pending.")
	}
}

func (dc *DiagnosticContext) FakeErr(err error) {
	select {
	case dc.chErr <- err:
	default:
		fmt.Fprintln(os.Stderr, "[diag] no landing pending.")
	}
}

func (dc *DiagnosticContext) FakeCo() {
	select {
	case dc.chCo <- struct{}{}:
	default:
		fmt.Fprintln(os.Stderr, "[diag] no corun pending.")
	}
}
