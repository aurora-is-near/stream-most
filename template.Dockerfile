FROM golang:1.26-alpine AS builder
ENV GOPRIVATE=github.com/aurora-is-near/*

RUN apk add --no-cache git

WORKDIR /build

COPY go.mod go.sum ./
RUN --mount=type=secret,id=GIT_AUTH_TOKEN \
    GIT_AUTH_TOKEN=$(cat /run/secrets/GIT_AUTH_TOKEN) && \
    git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/" && \
    go mod download

COPY . .
ARG APP
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app ./cmd/$APP

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app /usr/local/bin/app
RUN addgroup -S aurora && adduser --disabled-password --no-create-home -S aurora -G aurora
USER aurora

ENTRYPOINT [ "/usr/local/bin/app" ]
