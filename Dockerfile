FROM golang:1.24 as builder

ARG VERSION="1.0.0"
ARG REST_PORT="8080"
ARG GRPC_PORT="50051"

ENV GO111MODULE=on \
  CGO_ENABLED=0 \
  GOOS=linux \
  GOARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -ldflags "-X main.Version=$VERSION" -o location-service .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/location-service .

RUN addgroup -S appgroup 
RUN adduser -S appuser
RUN chown -R appuser:appgroup /app

EXPOSE $REST_PORT
EXPOSE $GRPC_PORT

CMD ["./location-service"]
