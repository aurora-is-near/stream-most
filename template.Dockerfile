FROM golang:1.26-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

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
