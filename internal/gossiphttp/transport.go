// Package gossiphttp implements an HTTP/2 transport for gossip.
//
// # Protocol
//
// Peers send two types of messages to each other:
//
//  1. /api/v1/ckit/transport/message sends a stream of messages to a peer. The
//     receiver does not respond with any messages.
//
//  2. /api/v1/ckit/transport/stream opens a bidirectional communication
//     channel to a peer, where both peers may send messages to each other. Once
//     either peer closes the connection, the stream is terminated.
//
// Both requests expect the Content-Type header to be set to
// application/x.ckit.
//
// Requests MUST be delivered over HTTP/2. HTTP/1.X requests will be rejected
// with HTTP 505 HTTP Version Not Supported.
//
// # Message Format
//
// All messages sent between peers have the same format:
//
//	+------------------------+
//	| Magic = 0xCC <1  byte> |
//	|------------------------|
//	| Data length  <2 bytes> |
//	|------------------------|
//	| Data         <n bytes> |
//	+------------------------+
//
// It is recommended that the message data size kept within the UDP MTU size,
// normally 1400 bytes. The message data size must not exeed 65,535 bytes.
package gossiphttp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rfratto/ckit/internal/queue"
)

// packetBufferSize is the maximum amount of packets that can be held in
// memory. Packets buffers are an LRU cache, so the oldest non-dequeued packet
// is discarded after a bufer is full.
const packetBufferSize = 1000

// API endpoints used for messaging.
var (
	baseRoute = "/api/v1/ckit/transport/"

	// messageEndpoint is used to send one or more messages to a peer.
	messageEndpoint = baseRoute + "message"

	// streamEndpoint is used to open a communication stream to a peer where they
	// can exchange larger amounts of information.
	streamEndpoint = baseRoute + "stream"
)

// Options controls the gossiphttp transport.
type Options struct {
	// Optional logger to use.
	Log log.Logger

	// Client to use for communicating to peers. Required. The Transport used by
	// the client must be able to handle HTTP2 requests for any peer.
	//
	// Note that TLS is not required for communication between peers. The
	// Client.Transport should be able to fall back to h2c for HTTP2 traffic when
	// connections over HTTPS are not used.
	Client *http.Client

	// Timeout to use when sending a packet.
	PacketTimeout time.Duration
}

// Transport is an HTTP/2 implementation of memberlist.Transport. Call
// NewTransport to create one.
type Transport struct {
	log     log.Logger
	opts    Options
	metrics *metrics

	// memberlist is designed for UDP, which is nearly non-blocking for writes.
	// We need to be able to emulate the same performance of passing messages, so
	// we write messages to buffered queues which are processed in the
	// background.
	inPacketQueue, outPacketQueue *queue.Queue

	// Generated after calling FinalAdvertiseAddr
	localAddr net.Addr
}

var _ memberlist.Transport = (*Transport)(nil)

// NewTransport returns a new Transport. Transports must be attached to an HTTP
// server so their endpoints are invoked. See [Handler] for more information.
func NewTransport(opts Options) (*Transport, prometheus.Collector, error) {
	if opts.Client == nil {
		return nil, nil, fmt.Errorf("HTTP client must be provided")
	}

	l := opts.Log
	if l == nil {
		l = log.NewNopLogger()
	}

	t := &Transport{
		log:     l,
		opts:    opts,
		metrics: newMetrics(),

		inPacketQueue:  queue.New(packetBufferSize),
		outPacketQueue: queue.New(packetBufferSize),
	}

	t.metrics.Add(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "cluster_transport_rx_packet_queue_length",
			Help: "Current number of unprocessed incoming packets",
		},
		func() float64 { return float64(t.inPacketQueue.Size()) },
	))
	t.metrics.Add(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "cluster_transport_tx_packet_queue_length",
			Help: "Current number of unprocessed outgoing packets",
		},
		func() float64 { return float64(t.outPacketQueue.Size()) },
	))

	// TODO(rfratto): goroutine to read from the queue in background and send
	// packets to peers.

	return &Transport{}, t.metrics, nil
}

// Handler returns the base HTTP route and handler for the Transport.
func (t *Transport) Handler() (route string, handler http.Handler) {
	mux := http.NewServeMux()
	mux.Handle(messageEndpoint, http.HandlerFunc(t.handleMessage))
	mux.Handle(streamEndpoint, http.HandlerFunc(t.handleStream))
	return baseRoute, mux
}

func (t *Transport) handleMessage(w http.ResponseWriter, r *http.Request) {
	var (
		recvTime   = time.Now()
		remoteAddr = parseRemoteAddr(r.RemoteAddr)
	)

	// Read each message until the request body has been fully consumed. Each
	// message is converted into a single packet.
	for {
		msg, err := readMessage(r.Body)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			level.Warn(t.log).Log("msg", "error reading packet from peer", "err", err)
			break
		}

		t.metrics.packetRxTotal.Inc()
		t.metrics.packetRxBytesTotal.Add(float64(len(msg)))

		// Enqueue the packet to be processed in the background. This allows HTTP
		// calls to have as low of a latency as possible to help keep things moving
		// along.
		t.inPacketQueue.Enqueue(&memberlist.Packet{
			Buf:       msg,
			From:      remoteAddr,
			Timestamp: recvTime,
		})
	}

	w.WriteHeader(http.StatusOK)
}

// parseRemoteAddr parses a ip:port string into a net.Addr. If the addr cannot
// be parsed, a default implementation for an "unknown" net.Addr is returned.
func parseRemoteAddr(addr string) net.Addr {
	remoteHost, remoteService, err := net.SplitHostPort(addr)
	if err != nil {
		return unknownAddr{}
	}

	remoteIP := net.ParseIP(remoteHost)
	if remoteIP == nil {
		return unknownAddr{}
	}

	remotePort, err := net.LookupPort("tcp", remoteService)
	if err != nil {
		return unknownAddr{}
	}

	return &net.TCPAddr{
		IP:   remoteIP,
		Port: remotePort,
	}
}

func (t *Transport) handleStream(w http.ResponseWriter, r *http.Request) {
	// TODO(rfratto): something
}

// FinalAdvertiseAddr returns the address this peer uses to advertise its
// connections.
func (t *Transport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	if ip == "" {
		return nil, 0, fmt.Errorf("no configured advertise address")
	} else if port == 0 {
		return nil, 0, fmt.Errorf("missing real listen port")
	}

	advertiseIP := net.ParseIP(ip)
	if advertiseIP == nil {
		return nil, 0, fmt.Errorf("failed to parse advertise ip %q", ip)
	}

	// Convert to IPv4 if possible.
	if ip4 := advertiseIP.To4(); ip4 != nil {
		advertiseIP = ip4
	}

	t.localAddr = &net.TCPAddr{IP: advertiseIP, Port: port}
	return advertiseIP, port, nil
}

// WriteTo enqueues a message b to be sent to the peer specified by addr. The
// message is delivered in the background asynchronously by the transport.
func (t *Transport) WriteTo(b []byte, addr string) (time.Time, error) {
	panic("NYI")
}

// PacketCh returns a channel of packets received from remote peers.
func (t *Transport) PacketCh() <-chan *memberlist.Packet {
	panic("NYI")
}

// DialTimeout opens a bidirectional communication channel to the specified
// peer address.
func (t *Transport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	panic("NYI")
}

// StreamCh returns a channel of bidirectional communication channels opened by
// remote peers.
func (t *Transport) StreamCh() <-chan net.Conn {
	panic("NYI")
}

// Shutdown terminates the transport.
func (t *Transport) Shutdown() error {
	panic("NYI")
}
