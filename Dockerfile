FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
RUN CGO_ENABLED=0 GOOS=linux go build -o coalesce ./cmd/web

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /build/coalesce .
COPY run.html ./run.html
EXPOSE 8080
ENTRYPOINT ["/app/coalesce"]
