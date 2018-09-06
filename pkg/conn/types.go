package conn

type WireError struct {
	msg string
}

func (e *WireError) Error() string {
	return e.msg
}

type Connection interface {
	Fire(code string)
	FireCoRun(code string, data chan []byte)

	Connect() (err error)

	Notif(code string) (err error)
	NotifCoRun(code string, data chan []byte) (err error)

	CoBegin() (err error)

	CoSendCoRun(code string) (err error)

	CoGet(code string) (result interface{}, err error)

	CoSendCode(code string) (err error)
	CoSendData(data chan []byte) (err error)

	CoRecvObj() (result interface{}, err error)
	CoRecvData(data chan []byte) (err error)

	CoEnd() (err error)

	Disconnect() (err error)
}
