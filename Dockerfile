############################
# STEP 1 build executable binary
############################
FROM golang:alpine AS builder
# Install git.
# Git is required for fetching the dependencies.
ENV GOPATH=/go
ENV PATH=$GOPATH:$PATH
RUN apk update && apk add --no-cache git
WORKDIR $GOPATH/src/github.com/Ido-Sheffer/KubeMQvsnats
COPY . .
# Fetch dependencies.
# Using go get.
RUN go get -d -v
# Build the binary.
RUN go build -o /go/bin/KubeMQvsnats
CMD [ "/bin/sh" ]