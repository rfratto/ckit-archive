package ckit_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/rfratto/ckit"
	"github.com/rfratto/ckit/peer"
	"github.com/rfratto/ckit/shard"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

func Example() {
	// Our cluster works over gRPC, so we must first create a gRPC server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()

	// We want to be able to perform consistent hashing against the state of the
	// cluster. We'll create a ring for our node to update.
	ring := shard.Ring(128)

	// Create a config to use for joining the cluster. The config must at least
	// have a unique name for the node in the cluster, and the address that other
	// nodes can connect to using gRPC.
	cfg := ckit.Config{
		// Name of the discoverer. Must be unique.
		Name: "first-node",

		// AdvertiseAddr will be the address shared with other nodes.
		AdvertiseAddr: lis.Addr().String(),

		// Cluster changes will be immediately synchronized with a sharder (when
		// provided).
		Sharder: ring,

		Log: log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)),
	}

	// We can create a node from our config with a gRPC server to use. Nodes do not
	// join the cluster until Start is called.
	node, err := ckit.NewNode(grpcServer, cfg)
	if err != nil {
		panic(err)
	}

	// Nodes can optionally emit events to any number of observers to notify when
	// the list of peers in the cluster has changed.
	//
	// Note that Observers are invoked in the background and so this function
	// might not always execute within this example.
	node.Observe(ckit.FuncObserver(func(peers []peer.Peer) (reregister bool) {
		names := make([]string, len(peers))
		for i, p := range peers {
			names[i] = p.Name
		}

		level.Info(cfg.Log).Log("msg", "peers changed", "new_peers", strings.Join(names, ","))
		return true
	}))

	// Run our gRPC server. This can only happen after the discoverer is created.
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Join the cluster with an initial set of peers to connect to. We're the only
	// node, so pass an empty string slice. Otherwise, we'd give the address of
	// another peer to connect to.
	err = node.Start(nil)
	if err != nil {
		panic(err)
	}
	defer node.Stop()

	// Nodes initially join the cluster in the Viewer state. We can move to the
	// Participant state to signal that we wish to participate in reading or
	// writing data.
	err = node.ChangeState(context.Background(), peer.StateParticipant)
	if err != nil {
		panic(err)
	}

	// Changing our state will have caused our sharder to be updated as well. We
	// can now look up the owner for a key. We should be the owner since we're
	// the only node.
	owners, err := ring.Lookup(shard.StringKey("some-key"), 1, shard.OpReadWrite)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Owner of some-key: %s\n", owners[0].Name)

	// Output:
	// Owner of some-key: first-node
}

func ExampleOverHTTP() {
	// Our cluster works over HTTP, so we must first create an HTTP server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	srv := &http.Server{
		Addr:    lis.Addr().String(),
		Handler: mux,
	}

	// We want to be able to perform consistent hashing against the state of the
	// cluster. We'll create a ring for our node to update.
	ring := shard.Ring(128)

	// Create a config to use for joining the cluster. The config must at least
	// have a unique name for the node in the cluster, and the address that other
	// nodes can connect to using gRPC.
	cfg := ckit.Config{
		// Name of the discoverer. Must be unique.
		Name: "first-node",

		// AdvertiseAddr will be the address shared with other nodes.
		// AdvertiseAddr: lis.Addr().String(),
		AdvertiseAddr: lis.Addr().String(),

		// Cluster changes will be immediately synchronized with a sharder (when
		// provided).
		Sharder: ring,

		Log: log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)),
	}

	// We can create a node from our config with a gRPC server to use. Nodes do not
	// join the cluster until Start is called.
	node, err := ckit.NewHTTPNode(mux, cfg)
	if err != nil {
		panic(err)
	}

	// Nodes can optionally emit events to any number of observers to notify when
	// the list of peers in the cluster has changed.
	//
	// Note that Observers are invoked in the background and so this function
	// might not always execute within this example.
	node.Observe(ckit.FuncObserver(func(peers []peer.Peer) (reregister bool) {
		names := make([]string, len(peers))
		for i, p := range peers {
			names[i] = p.Name
		}

		level.Info(cfg.Log).Log("msg", "peers changed", "new_peers", strings.Join(names, ","))
		return true
	}))

	// Run our gRPC server. This can only happen after the discoverer is created.
	go func() {
		err := srv.Serve(lis)
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	defer srv.Shutdown(context.Background())

	// Join the cluster with an initial set of peers to connect to. We're the only
	// node, so pass an empty string slice. Otherwise, we'd give the address of
	// another peer to connect to.
	err = node.Start(nil)
	if err != nil {
		panic(err)
	}
	defer node.Stop()

	// Nodes initially join the cluster in the Viewer state. We can move to the
	// Participant state to signal that we wish to participate in reading or
	// writing data.
	err = node.ChangeState(context.Background(), peer.StateParticipant)
	if err != nil {
		panic(err)
	}

	// Changing our state will have caused our sharder to be updated as well. We
	// can now look up the owner for a key. We should be the owner since we're
	// the only node.
	owners, err := ring.Lookup(shard.StringKey("some-key"), 1, shard.OpReadWrite)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Owner of some-key: %s\n", owners[0].Name)

	// Output:
	// Owner of some-key: first-node
}

func TestHTTPNodes_UseHTTPTest(t *testing.T) {
	// Define N nodes, each with its own HTTP server and ckit config
	numNodes := 3
	// var servers []*http.Server
	var configs []ckit.Config
	var nodes []*ckit.Node
	var listeners []net.Listener
	var muxs []*http.ServeMux

	for i := 0; i < numNodes; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		listeners = append(listeners, lis)

		muxs = append(muxs, http.NewServeMux())

		configs = append(configs, ckit.Config{
			AdvertiseAddr: lis.Addr().String(),
			Sharder:       shard.Rendezvous(),
			Log:           log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)),
		})
		configs[i].Name = fmt.Sprintf("node-%d", i)

		n, err := ckit.NewHTTPNode(muxs[i], configs[i])
		if err != nil {
			panic(err)
		}

		nodes = append(nodes, n)
		nodes[i].Observe(ckit.FuncObserver(func(peers []peer.Peer) (reregister bool) { return true }))
	}

	// Start all HTTP servers
	go func() {
		for i := 0; i < numNodes; i++ {
			srv := httptest.NewUnstartedServer(h2c.NewHandler(muxs[i], &http2.Server{}))
			srv.Listener.Close()
			srv.Listener = listeners[i]
			srv.Start()
		}
	}()

	// Start first node to initialize the cluster; change its state to Participant
	err := nodes[0].Start(nil)
	if err != nil {
		panic(err)
	}
	defer nodes[0].Stop()

	err = nodes[0].ChangeState(context.Background(), peer.StateParticipant)
	if err != nil {
		panic(err)
	}

	// Start up the rest of the nodes in sequence
	for i := 1; i < numNodes; i++ {
		err := nodes[i].Start([]string{listeners[0].Addr().String()})
		require.NoError(t, err)
		err = nodes[i].ChangeState(context.Background(), peer.StateParticipant)
		require.NoError(t, err)
	}

	// Inspect the cluster state.
	for i := 0; i < numNodes; i++ {
		require.Len(t, nodes[i].Peers(), numNodes)
	}
}

func TestHTTPNodes_UseProper(t *testing.T) {
	// Define N nodes, each with its own HTTP server and ckit config
	numNodes := 3

	var servers []*http.Server
	var configs []ckit.Config
	var nodes []*ckit.Node
	var listeners []net.Listener
	var muxs []*http.ServeMux

	for i := 0; i < numNodes; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		listeners = append(listeners, lis)
		muxs = append(muxs, http.NewServeMux())

		configs = append(configs, ckit.Config{
			AdvertiseAddr: lis.Addr().String(),
			Sharder:       shard.Rendezvous(),
			Log:           log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)),
		})
		configs[i].Name = fmt.Sprintf("node-%d", i)

		n, err := ckit.NewHTTPNode(muxs[i], configs[i])
		if err != nil {
			panic(err)
		}

		nodes = append(nodes, n)
		nodes[i].Observe(ckit.FuncObserver(func(peers []peer.Peer) (reregister bool) { return true }))
	}

	for i := 0; i < numNodes; i++ {
		servers = append(servers, &http.Server{
			Addr:    listeners[i].Addr().String(),
			Handler: h2c.NewHandler(muxs[i], &http2.Server{}),
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	}

	// Start all HTTP servers
	for i := 0; i < numNodes; i++ {
		go func(i int) {
			defer servers[i].Close()
			defer listeners[i].Close()
			servers[i].Serve(listeners[i])
		}(i)
	}
	time.Sleep(1 * time.Second)

	// Start first node to initialize the cluster; change its state to Participant
	err := nodes[0].Start(nil)
	if err != nil {
		panic(err)
	}
	defer nodes[0].Stop()

	err = nodes[0].ChangeState(context.Background(), peer.StateParticipant)
	if err != nil {
		panic(err)
	}

	// Start up the rest of the nodes in sequence
	for i := 1; i < numNodes; i++ {
		err := nodes[i].Start([]string{listeners[0].Addr().String()})
		require.NoError(t, err)
		err = nodes[i].ChangeState(context.Background(), peer.StateParticipant)
		require.NoError(t, err)
	}
	// time.Sleep(500 * time.Second)

	fmt.Println(nodes[0].Peers())
	fmt.Println(nodes[1].Peers())
	fmt.Println(nodes[2].Peers())

	// Inspect the cluster state.
	for i := 0; i < numNodes; i++ {
		require.Len(t, nodes[i].Peers(), numNodes)
	}
}
