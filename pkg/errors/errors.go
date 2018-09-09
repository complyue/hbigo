package errors

type WireError struct {
	Err string
}

func (e WireError) Error() string {
	return e.Err
}

type UsageError struct {
	Err string
}

func (e UsageError) Error() string {
	return e.Err
}
