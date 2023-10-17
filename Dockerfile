FROM golang:1.21-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum* ./
ENV CGO_ENABLED=0 GOOS=linux GOWORK=off
RUN --mount=type=cache,target=/go/pkg/mod go mod download
RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /root/eventing-api ./cmd/api/main.go

FROM alpine AS final
WORKDIR /root
COPY --from=builder /root/eventing-api /root/eventing-api
ENTRYPOINT ["/root/eventing-api"]
