# The server image carries Two: the node stage builds studio/two, then its
# output is copied where cmd/web's go:embed expects it — one image, one
# artifact, with the same UI source used by the local Vite loop.
FROM node:22-alpine AS ui

WORKDIR /build/studio
COPY studio/package.json studio/package-lock.json ./
RUN npm ci --no-fund --no-audit
COPY studio/ ./
RUN npm run build:two

FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY --from=ui /build/studio/dist/two ./cmd/web/dist
RUN CGO_ENABLED=0 GOOS=linux go build -o coalesce ./cmd/web

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /build/coalesce .
COPY run.html ./run.html
EXPOSE 8080
ENTRYPOINT ["/app/coalesce"]
