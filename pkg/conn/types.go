package conn

type WireError struct {
	msg string
}

func (e *WireError) Error() string {
	return e.msg
}

type UsageError struct {
	msg string
}

func (e *UsageError) Error() string {
	return e.msg
}

type Connection interface {
	Connect() (err error)

	Fire(code string)
	FireCoRun(code string, data <-chan []byte)

	Notif(code string) (err error)
	NotifCoRun(code string, data <-chan []byte) (err error)

	Co() (hbic Connection)
	coDone()

	CoSendCoRun(code string, data <-chan []byte) (err error)

	CoGet(code string) (result interface{}, err error)

	CoSendCode(code string) (err error)
	CoSendData(data <-chan []byte) (err error)

	CoRecvObj() (result interface{}, err error)
	CoRecvData(data <-chan []byte) (err error)

	Disconnect() (err error)
}

func CoDone(hbic Connection) {
	hbic.coDone()
}
