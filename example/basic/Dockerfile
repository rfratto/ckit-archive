FROM golang:1.20-alpine3.17 as builder

COPY . /src
WORKDIR /src/example/basic

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -o /basic .

FROM alpine:3.17

COPY --from=builder /basic /usr/bin/basic

ENTRYPOINT ["/usr/bin/basic"]
