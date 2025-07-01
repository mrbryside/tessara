FROM golang:1.24.3-alpine AS builder

# install git, curl, air
RUN apk add --no-cache git curl gcc libc-dev && \
    go install github.com/air-verse/air@latest && \
    go install github.com/go-delve/delve/cmd/dlv@latest
