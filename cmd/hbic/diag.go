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
		HoContext: hbi.NewHoContext(),
		InbHist:   []string{},
		chResult:  make(chan interface{}),
		chVoid:    make(chan struct{}),
		chErr:     make(chan error),
	}
}

type DiagnosticContext struct {
	hbi.HoContext

	// the active delegate
	Delegate hbi.HoContext

	// history of inbound code
	InbHist    []string
	NextToLand int

	// history of landed result
	LandHist []interface{}

	// channel to send outbound objects
	chResult chan interface{}
	chVoid   chan struct{}
	chErr    chan error
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
	for dc.NextToLand < len(dc.InbHist) {
		result, _, err := dele.Exec(dc.InbHist[dc.NextToLand])
		if err != nil {
			glog.Error(err)
			return
		}
		dc.NextToLand++
		dc.LandHist = append(dc.LandHist, result)
	}
}

// hijack code landing logic
func (dc *DiagnosticContext) Exec(code string) (result interface{}, ok bool, err error) {
	dc.InbHist = append(dc.InbHist, code)

	if dc.NextToLand == len(dc.InbHist)-1 && dc.Delegate != nil {
		// fluent landing by delegate, continue it
		result, ok, err = dc.Delegate.Exec(code)
		if err == nil {
			glog.Error(err)
			return
		}
		dc.NextToLand++
		dc.LandHist = append(dc.LandHist, result)
		return
	}

	fmt.Fprintf(os.Stderr, "In[%d]:\n#-#-#\n%s\n*-*-*\n", len(dc.InbHist), code)

	select {
	case err = <-dc.chErr:
		return
	case result = <-dc.chResult:
	case <-dc.chVoid:
		ok = false
	}

	dc.NextToLand++
	dc.LandHist = append(dc.LandHist, result)

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
		fmt.Fprintln(os.Stderr, "[diag] no landing pending.")
	}
}

func (dc *DiagnosticContext) FakeVoid() {
	select {
	case dc.chVoid <- struct{}{}:
	default:
		fmt.Fprintln(os.Stderr, "[diag] no landing pending.")
	}
}

func (dc *DiagnosticContext) FakeErr(err error) {
	select {
	case dc.chErr <- err:
	default:
		fmt.Fprintln(os.Stderr, "[diag] no landing pending.")
	}
}
