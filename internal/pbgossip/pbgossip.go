// Package pbgossip implements a memberlist.Transport implementation using
// protobuf and connect (HTTP/2). This would probably be a bad idea for
// transitional uses of memberlist, but ckit only gossips member status, so the
// overhead of using HTTP/2 for everything should be minimal.
package pbgossip

//go:generate protoc --go_out=. --go_opt=module=github.com/rfratto/ckit/internal/pbgossip --connect-go_out=. --connect-go_opt=module=github.com/rfratto/ckit/internal/pbgossip ./pbgossip.proto

// TODO(rfratto): do we need connect here at all? can we "just" use http2
// directly since the proto is so lightweight?
//
// The answer is yes, we can "just" use http2 by sending a io.Reader as the
// request body and not closing it, and the same with "just" having a response
// body which streams over time.
//
// We will need to make sure the connection is over HTTP/2 or otherwise use
// some kind of response timeout.
