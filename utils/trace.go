package utils

import (
	"io/ioutil"
	"log"
)

// Internal levels of library output that are initialised to not print
// anything but can be overridden by programmer
var (
	ERROR    *log.Logger
	CRITICAL *log.Logger
	WARN     *log.Logger
	DEBUG    *log.Logger
)

func init() {
	ERROR = log.New(ioutil.Discard, "", 0)
	CRITICAL = log.New(ioutil.Discard, "", 0)
	WARN = log.New(ioutil.Discard, "", 0)
	DEBUG = log.New(ioutil.Discard, "", 0)
}
