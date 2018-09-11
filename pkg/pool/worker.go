package pool

import (
	"fmt"
	"github.com/complyue/hbigo"
)

/*
A service hosting worker process should communicate with service pool master at TCP address specified as `-team`,
using a context struct embedding this interface, constructed by `NewWorkerHoContext()`,
and optionally implement (override) methods defined by this interface.

And once it get ready for HBI service hosting, it should tell the pool master its service port by:
	ctx.Ho().Po().Notif(`WorkerOnline(<pid int>, <procPort int>)`)

*/
type WorkerHoContext interface {
	hbi.HoContext

	// should prepare for the session and send verbatim session id string back via `PoToPeer().CoSendCode()`
	PrepareSession(session string)
}

func NewWorkerHoContext() WorkerHoContext {
	return &workerHoContext{
		hbi.NewHoContext(),
	}
}

type workerHoContext struct {
	hbi.HoContext
}

// send verbatim session id string back, to confirm the session prepared
func (ctx *workerHoContext) PrepareSession(session string) {
	ctx.PoToPeer().CoSendCode(fmt.Sprintf("%#v", session))
}
