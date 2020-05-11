FROM golang:1.14 as builder

ARG version
ARG builddate

WORKDIR /go/src/gitlab.com/devopsworks/tools/dw-query-digest

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN GOOS=linux \
    GOARCH=amd64 \
    CGO_ENABLED=0 \
    go build \
        -tags release \
        -ldflags '-w -extldflags "-static" -X main.Version=${version} -X main.BuildDate=${builddate}' -a \
        -o /go/bin/dw-query-digest

FROM scratch

COPY --from=builder /go/bin/dw-query-digest /usr/local/bin/dw-query-digest

ENTRYPOINT ["/usr/local/bin/dw-query-digest"]