# --- build stage ---
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Download dependencies first (cached layer).
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kvstore ./cmd/kvstore

# --- runtime stage ---
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /kvstore /usr/local/bin/kvstore

# 7001 = Raft RPC port  |  8081 = KVStore HTTP API port (default)
# The actual ports are set via HTTP_ADDR and RAFT_RPC_ADDR env vars.
EXPOSE 7001 8081

ENTRYPOINT ["kvstore"]
