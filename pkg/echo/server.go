//go:generate hbic

/*
Echo server and client for test purpose
*/
package echo

var LastMsg string

func Ppp(msg string) string {
	if len(msg) > 0 {
		LastMsg = msg
	}
	println("ppp - msg: ", LastMsg)
	return LastMsg
}
