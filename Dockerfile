# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY cmd/ ./cmd/

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -o coalesce cmd/web/main.go

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/coalesce .

# Copy static files (for now just placeholders)
COPY run.html ./run.html

EXPOSE 8080

ENTRYPOINT ["/app/coalesce"]
