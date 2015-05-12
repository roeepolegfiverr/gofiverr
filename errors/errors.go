// taken from dropbox - https://raw.githubusercontent.com/dropbox/godropbox/master/errors/errors.go
//
package errors

// This module implements functions which manipulate errors and provide stack
// trace information.
//
// NOTE: This package intentionally mirrors the standard "errors" module.
// All Fiverr code should use this.

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
)

// This interface exposes additional information about the error.
type FiverrError interface {
	// This returns the error message without the stack trace.
	GetMessage() string

	// This returns the stack trace without the error message.
	GetStack() string

	// This returns the stack trace's context.
	GetContext() string

	// This returns the wrapped error.  This returns nil if this does not wrap
	// another error.
	GetInner() error

	// Implements the built-in error interface.
	Error() string
}

// Standard struct for general types of errors.
//
// For an example of custom error type, look at databaseError/newDatabaseError
// in errors_test.go.
type FiverrBaseError struct {
	Msg     string
	Stack   string
	Context string
	inner   error
}

// This returns the error string without stack trace information.
func GetMessage(err interface{}) string {
	switch e := err.(type) {
	case FiverrError:
		dberr := FiverrError(e)
		ret := []string{}
		for dberr != nil {
			ret = append(ret, dberr.GetMessage())
			d := dberr.GetInner()
			if d == nil {
				break
			}
			var ok bool
			dberr, ok = d.(FiverrError)
			if !ok {
				ret = append(ret, d.Error())
				break
			}
		}
		return strings.Join(ret, " ")
	case runtime.Error:
		return runtime.Error(e).Error()
	default:
		return "Passed a non-error to GetMessage"
	}
}

// This returns a string with all available error information, including inner
// errors that are wrapped by this errors.
func (e *FiverrBaseError) Error() string {
	return DefaultError(e)
}

// This returns the error message without the stack trace.
func (e *FiverrBaseError) GetMessage() string {
	return e.Msg
}

// This returns the stack trace without the error message.
func (e *FiverrBaseError) GetStack() string {
	return e.Stack
}

// This returns the stack trace's context.
func (e *FiverrBaseError) GetContext() string {
	return e.Context
}

// This returns the wrapped error, if there is one.
func (e *FiverrBaseError) GetInner() error {
	return e.inner
}

// This returns a new FiverrBaseError initialized with the given message and
// the current stack trace.
func New(msg string) FiverrError {
	stack, context := StackTrace()
	return &FiverrBaseError{
		Msg:     msg,
		Stack:   stack,
		Context: context,
	}
}

// Same as New, but with fmt.Printf-style parameters.
func Newf(format string, args ...interface{}) FiverrError {
	stack, context := StackTrace()
	return &FiverrBaseError{
		Msg:     fmt.Sprintf(format, args...),
		Stack:   stack,
		Context: context,
	}
}

// Wraps another error in a new FiverrBaseError.
func Wrap(err error, msg string) FiverrError {
	stack, context := StackTrace()
	return &FiverrBaseError{
		Msg:     msg,
		Stack:   stack,
		Context: context,
		inner:   err,
	}
}

// Same as Wrap, but with fmt.Printf-style parameters.
func Wrapf(err error, format string, args ...interface{}) FiverrError {
	stack, context := StackTrace()
	return &FiverrBaseError{
		Msg:     fmt.Sprintf(format, args...),
		Stack:   stack,
		Context: context,
		inner:   err,
	}
}

// A default implementation of the Error method of the error interface.
func DefaultError(e FiverrError) string {
	// Find the "original" stack trace, which is probably the most helpful for
	// debugging.
	errLines := make([]string, 1)
	var origStack string
	errLines[0] = "ERROR:"
	fillErrorInfo(e, &errLines, &origStack)
	errLines = append(errLines, "")
	errLines = append(errLines, "ORIGINAL STACK TRACE:")
	errLines = append(errLines, origStack)
	return strings.Join(errLines, "\n")
}

// Fills errLines with all error messages, and origStack with the inner-most
// stack.
func fillErrorInfo(err error, errLines *[]string, origStack *string) {
	if err == nil {
		return
	}

	derr, ok := err.(FiverrError)
	if ok {
		*errLines = append(*errLines, derr.GetMessage())
		*origStack = derr.GetStack()
		fillErrorInfo(derr.GetInner(), errLines, origStack)
	} else {
		*errLines = append(*errLines, err.Error())
	}
}

// Returns a copy of the error with the stack trace field populated and any
// other shared initialization; skips 'skip' levels of the stack trace.
//
// NOTE: This panics on any error.
func stackTrace(skip int) (current, context string) {
	// grow buf until it's large enough to store entire stack trace
	buf := make([]byte, 128)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, len(buf)*2)
	}

	// Returns the index of the first occurrence of '\n' in the buffer 'b'
	// starting with index 'start'.
	//
	// In case no occurrence of '\n' is found, it returns len(b). This
	// simplifies the logic on the calling sites.
	indexNewline := func(b []byte, start int) int {
		if start >= len(b) {
			return len(b)
		}
		searchBuf := b[start:]
		index := bytes.IndexByte(searchBuf, '\n')
		if index == -1 {
			return len(b)
		}
		return (start + index)
	}

	// Strip initial levels of stack trace, but keep header line that
	// identifies the current goroutine.
	var strippedBuf bytes.Buffer
	index := indexNewline(buf, 0)
	if index != -1 {
		strippedBuf.Write(buf[:index])
	}

	// Skip lines.
	for i := 0; i < skip; i++ {
		index = indexNewline(buf, index+1)
		index = indexNewline(buf, index+1)
	}

	isDone := false
	startIndex := index
	lastIndex := index
	for !isDone {
		index = indexNewline(buf, index+1)
		if (index - lastIndex) <= 1 {
			isDone = true
		} else {
			lastIndex = index
		}
	}
	strippedBuf.Write(buf[startIndex:index])
	return strippedBuf.String(), string(buf[index:])
}

// This returns the current stack trace string.  NOTE: the stack creation code
// is excluded from the stack trace.
func StackTrace() (current, context string) {
	return stackTrace(3)
}
