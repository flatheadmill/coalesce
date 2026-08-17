# The server image carries the UI: the node stage builds ui/ into the
# location cmd/web's go:embed expects, so the built output is embedded and
# versioned with the server — one image, one artifact, no separate deploy.
FROM node:22-alpine AS ui

WORKDIR /build/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-fund --no-audit
COPY ui/ ./
RUN npm run build

FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY --from=ui /build/cmd/web/dist ./cmd/web/dist
RUN CGO_ENABLED=0 GOOS=linux go build -o coalesce ./cmd/web

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /build/coalesce .
COPY run.html ./run.html
EXPOSE 8080
ENTRYPOINT ["/app/coalesce"]
