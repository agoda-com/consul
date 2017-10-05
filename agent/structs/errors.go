package structs

import (
	"errors"
	"strings"
)

const (
	errNoLeader                   = "No cluster leader"
	errNoDCPath                   = "No path to datacenter"
	errNoServers                  = "No known Consul servers"
	errNotReadyForConsistentReads = "Not ready to serve consistent reads"
	errSegmentsNotSupported       = "Network segments are not supported in this version of Consul"
	errRPCRateExceeded            = "RPC rate limit exceeded"
	errPatternValidation          = "match pattern"
	errPatternError               = "parsing regexp"
)

var (
	ErrNoLeader                   = errors.New(errNoLeader)
	ErrNoDCPath                   = errors.New(errNoDCPath)
	ErrNoServers                  = errors.New(errNoServers)
	ErrNotReadyForConsistentReads = errors.New(errNotReadyForConsistentReads)
	ErrSegmentsNotSupported       = errors.New(errSegmentsNotSupported)
	ErrRPCRateExceeded            = errors.New(errRPCRateExceeded)
	ErrPatternValidation          = errors.New(errPatternValidation)
	ErrPatternError               = errors.New(errPatternError)
)

func IsErrRPCRateExceeded(err error) bool {
	return strings.Contains(err.Error(), errRPCRateExceeded)
}

func IsErrPatternValidation(err error) bool {
	return strings.Contains(err.Error(), errPatternValidation)
}

func IsErrPatternError(err error) bool {
	return strings.Contains(err.Error(), errPatternError)
}
