FROM golang:1.11 as builder

WORKDIR /go/src/github.com/devops-works/dw-query-digest/
COPY . /go/src/github.com/devops-works/dw-query-digest/

RUN GOPATH=/go/src \
    GOOS=linux \
    GOARCH=amd64 \
    CGO_ENABLED=0 \
    go build -ldflags '-w -extldflags "-static"' -o /go/bin/dw-query-digest

RUN ls /go/bin/dw-query-digest

FROM scratch

COPY --from=builder /go/bin/dw-query-digest /usr/local/bin/dw-query-digest

ENTRYPOINT ["/usr/local/bin/dw-query-digest"]