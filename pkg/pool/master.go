package pool

import "github.com/complyue/hbigo"

func newMaster4Consumer() hbi.HoContext {
	return &master4consumer{
		HoContext: hbi.NewHoContext(),
	}
}

type master4consumer struct {
	hbi.HoContext
}

func (master *master4consumer) AssignProc(session string, sticky bool) {

}
func (master *master4consumer) ReleaseProc() {

}

func newMaster4Worker() hbi.HoContext {
	return &master4worker{
		HoContext: hbi.NewHoContext(),
	}
}

type master4worker struct {
	hbi.HoContext
}

func (m4w *master4worker) WorkerOnline(pid int) {

}
