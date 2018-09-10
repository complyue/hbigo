package pool

import "github.com/complyue/hbigo"

func newWorker4Master() hbi.HoContext {
	return &worker4master{
		HoContext: hbi.NewHoContext(),
	}
}

type worker4master struct {
	hbi.HoContext
}

func (m4w *worker4master) Retire(pid int) {

}
