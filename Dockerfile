FROM golang:1.16.13-stretch AS builder
WORKDIR /kafka-shovel
ENV GO111MODULE=on

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build

EXPOSE 8080

FROM alpine:3.11.0
RUN apk add --update --no-cache ca-certificates && rm -rf /var/cache/apk/*
COPY --from=builder /kafka-shovel/kafka-shovel /

ENTRYPOINT ./kafka-shovel
