package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rfratto/ckit"
	"github.com/rfratto/ckit/advertise"
	"github.com/rfratto/ckit/peer"
	"github.com/rfratto/ckit/shard"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func main() {
	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	var (
		nodeName, _           = os.Hostname()
		listenAddr            = "0.0.0.0:8080"
		joinAddrs   sliceFlag = []string{}
	)

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&nodeName, "node-name", nodeName, "Name to join the cluster with. Must be unique.")
	fs.StringVar(&listenAddr, "listen-addr", listenAddr, "Address to listen for traffic on.")
	fs.Var(&joinAddrs, "join-addrs", "Peers to join on startup.")

	if err := fs.Parse(os.Args[1:]); err != nil {
		level.Error(l).Log("msg", "failed to parse flags", "err", err)
		os.Exit(1)
	}

	// Validate flags.
	switch {
	case nodeName == "":
		level.Error(l).Log("msg", "-node-name flag must not be empty")
		os.Exit(1)
	case listenAddr == "":
		level.Error(l).Log("msg", "-listen-addr flag must not be empty")
		os.Exit(1)
	}

	advertiseAddr, _ := advertise.FirstAddress(advertise.DefaultInterfaces)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		level.Error(l).Log("msg", "failed to listen for traffic", "err", err)
		return
	}

	var (
		client = &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}

		nodeConfig = ckit.Config{
			Name:          nodeName,
			AdvertiseAddr: fmt.Sprintf("%s:%d", advertiseAddr, lis.Addr().(*net.TCPAddr).Port),
			Sharder:       shard.Ring(128),
			Log:           log.With(l, "component", "ckit"),
		}
	)

	node, err := ckit.NewNode(client, nodeConfig)
	if err != nil {
		level.Error(l).Log("msg", "failed to build node", "err", err)
		os.Exit(1)
	}

	node.Observe(ckit.FuncObserver(func(peers []peer.Peer) (reregister bool) {
		names := make([]string, len(peers))
		for i, p := range peers {
			names[i] = p.Name
		}

		level.Info(l).Log("msg", "peers changed", "new_peers", strings.Join(names, ","))
		return true
	}))

	// Register node metrics.
	prometheus.DefaultRegisterer.MustRegister(node.Metrics())

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Worker for HTTP server.
	wg.Add(1)
	{
		mux := http.NewServeMux()
		mux.Handle(node.Handler())
		mux.Handle("/metrics", promhttp.Handler())

		srv := &http.Server{
			Handler: h2c.NewHandler(mux, &http2.Server{}),
		}

		go func() {
			defer wg.Done()
			defer cancel()

			level.Info(l).Log("msg", "listening for traffic", "addr", lis.Addr())
			_ = srv.Serve(lis)
		}()
		defer func() {
			level.Info(l).Log("msg", "terminating server")
			_ = srv.Shutdown(context.Background())
		}()
	}

	if err := node.Start(joinAddrs); err != nil {
		level.Error(l).Log("msg", "failed to start node")
		return
	}
	defer node.Stop()

	// Worker for waiting for a cancellation signal.
	wg.Add(1)
	{
		go func() {
			defer wg.Done()
			defer cancel()

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt)
			defer signal.Stop(ch)

			select {
			case <-ch:
				level.Info(l).Log("msg", "cancellation signal received")
			case <-ctx.Done():
				// other worker exited; return.
				return
			}
		}()
	}

	// Change to a participant.
	if err := node.ChangeState(ctx, peer.StateParticipant); err != nil {
		level.Error(l).Log("msg", "failed to change state", "err", err)
	}

	<-ctx.Done()
}

type sliceFlag []string

var _ flag.Value

func (sf sliceFlag) String() string {
	return strings.Join(sf, ",")
}

func (sf *sliceFlag) Set(in string) error {
	*sf = strings.Split(in, ",")
	return nil
}
