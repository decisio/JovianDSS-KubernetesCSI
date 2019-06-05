package rest

import (
	"fmt"
)

const (
	RestFailureUnknown        = 1
	RestResourceBusy          = 2
	RestRequestMalfunction    = 3
	RestResourceDNE           = 4
	RestUnableToConnect       = 5
	RestRPM                   = 6 // Response Processing malfunction
	RestStorageFailureUnknown = 7
	RestObjectExists          = 8
)

type RestError interface {
	Error() (out string)
	GetCode() int
}

type restError struct {
	msg  string
	code int
}

//TODO: Refactor to move logging of error message in this func
func GetError(c int, m string) RestError {
	out := restError{
		code: c,
		msg:  m,
	}
	return &out
}

func (err *restError) Error() (out string) {

	switch (*err).code {

	case RestResourceBusy:
		out = fmt.Sprint("Resource is busy. %s", err.msg)

	case RestRequestMalfunction:
		out = fmt.Sprintf("Malfunction: %s", err.msg)

	case RestRPM:
		out = fmt.Sprintf("Failure during processing response from server: %s", err.msg)
	case RestObjectExists:
		out = fmt.Sprintf("Object exists: %s", err.msg)

	default:
		out = fmt.Sprint("Unknown internal Error. %s", err.msg)

	}
	return out
}

func (err *restError) GetCode() int {
	return err.code

}
