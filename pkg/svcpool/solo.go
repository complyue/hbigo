package svcpool

// embed this struct with a static service address for simplest service resolving
type StaticRegistry struct {
	// service address to resolve to
	ServiceAddr string
}

func (reg StaticRegistry) AssignProc(session string, sticky bool) (procAddr string) {
	return reg.ServiceAddr
}

func (reg StaticRegistry) ReleaseProc(procAddr string) (idle bool) {
	return
}
