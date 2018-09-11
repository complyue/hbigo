package errors

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
)

var (
	New    = errors.New
	Errorf = errors.Errorf
	Wrap   = errors.Wrap
	Wrapf  = errors.Wrapf
)

// github.com/pkg/errors can be formatted with rich information, including stacktrace, see:
// 	https://godoc.org/github.com/pkg/errors#hdr-Formatted_printing_of_errors
type richError interface {
	error
	fmt.Formatter
}

func RichError(err interface{}) error {
	if err == nil {
		return nil
	}
	switch err := err.(type) {
	case richError:
		return err
	case error:
		return errors.Wrap(err, err.Error()).(richError)
	default:
		return errors.New(fmt.Sprintf("%s", err)).(richError)
	}
}

func NewPacketError(errOmsg interface{}, pkt struct{ WireDir, Payload string }) *PacketError {
	switch e := errOmsg.(type) {
	case richError:
		return &PacketError{e, pkt}
	default:
		return &PacketError{RichError(e).(richError), pkt}
	}
}

type PacketError struct {
	richError

	pkt struct {
		WireDir string
		Payload string
	}
}

func (pe *PacketError) Format(s fmt.State, verb rune) {
	pe.richError.Format(s, verb)
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, "\n**hbi[#")
			io.WriteString(s, pe.pkt.WireDir)
			io.WriteString(s, "]hbi**\n")
			io.WriteString(s, pe.pkt.Payload)
			io.WriteString(s, "\n**hbi---hbi**\n")
		}
	}
}

func NewWireError(msg string) *WireError {
	return &WireError{errors.New(msg).(richError)}
}

type WireError struct {
	richError
}

func NewUsageError(msg string) *UsageError {
	return &UsageError{errors.New(msg).(richError)}
}

type UsageError struct {
	richError
}
