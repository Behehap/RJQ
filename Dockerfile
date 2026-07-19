# Stage 1: Build the binary.
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o rjq ./cmd/server

# Stage 2: Minimal runtime image.
FROM alpine:3.21

RUN apk add --no-cache sqlite-libs

WORKDIR /app
COPY --from=builder /app/rjq .
COPY config.yaml .

EXPOSE 8080

CMD ["./rjq"]