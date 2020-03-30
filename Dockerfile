FROM golang:1.14.1-alpine3.11 AS builder
LABEL maintainer="Onur Yilmaz <ahmet.onur.yilmazz@gmail.com>"
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
