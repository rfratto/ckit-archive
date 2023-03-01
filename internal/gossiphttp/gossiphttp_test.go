package gossiphttp_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func Test(t *testing.T) {
	t.Skip()

	echoHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r.Proto)

		w.Header().Set("Content-Type", "application/nothing")
		w.WriteHeader(http.StatusOK)

		for {
			buf := make([]byte, 1024)
			sz, err := r.Body.Read(buf)
			if err != nil {
				fmt.Println(err)
				break
			}

			message := string(buf[:sz])

			fmt.Println(message)

			_, err = fmt.Fprintf(w, "Echo: %s", message)
			if err != nil {
				fmt.Println(err)
			}

			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})

	srv := httptest.NewServer(h2c.NewHandler(echoHandler, &http2.Server{}))
	defer srv.Close()

	pr, pw := io.Pipe()

	go func() {
		cli := &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		}

		req, err := http.NewRequest(http.MethodPost, srv.URL, pr)
		if err != nil {
			fmt.Println(err)
			return
		}

		resp, err := cli.Do(req)
		if err != nil {
			fmt.Println(err)
			return
		}

		for {
			buf := make([]byte, 1024)
			sz, err := resp.Body.Read(buf)

			if sz > 0 {
				message := string(buf[:sz])
				fmt.Println(message)
			}

			if err != nil {
				fmt.Println(sz, err)
				break
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	tick := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			_ = pw.Close()
			time.Sleep(time.Second)
			return
		case <-tick.C:
			fmt.Fprint(pw, "Hello, world!")
		}
	}
}
