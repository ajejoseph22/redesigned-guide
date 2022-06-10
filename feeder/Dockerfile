FROM golang:1.17 as builder

ENV HOME /app
ENV CGO_ENABLED 0
ENV GOOS linux

RUN mkdir /app
ADD . /app
WORKDIR /app

RUN go mod download
RUN go build -o main .

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/main .
COPY --from=builder /app/.env .

CMD ["./main"]