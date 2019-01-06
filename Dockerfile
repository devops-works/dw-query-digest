FROM golang:1.11 as builder

ENV GO111MODULE=on

ARG version
ARG builddate

WORKDIR /go/src/github.com/devops-works/dw-query-digest/

COPY go.mod .
COPY go.sum .

RUN go mod download

# COPY . /go/src/github.com/devops-works/dw-query-digest/

COPY . .

# RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go install -a -tags netgo -ldflags '-w -extldflags "-static"' -o /go/bin/dw-query-digest

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