FROM golang:1.24 as builder

ARG VERSION="1.0.0"
ARG GRPC_PORT="50051"

ENV GO111MODULE=on \
  CGO_ENABLED=0 \
  GOOS=linux \
  GOARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -ldflags "-X main.Version=$VERSION" -o gokv .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/gokv /app/config.yml .

RUN addgroup -S appgroup 
RUN adduser -S appuser
RUN chown -R appuser:appgroup /app

EXPOSE $GRPC_PORT

CMD ["./gokv"]
