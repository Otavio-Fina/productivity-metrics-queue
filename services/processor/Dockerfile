 # ---- build stage ----
  FROM golang:1.25 AS builder

  WORKDIR /app

  COPY go.* ./
  RUN go mod download

  COPY . .
  RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /processor ./cmd

  # ---- final stage ----
  FROM scratch

  COPY --from=builder /processor /processor

  ENTRYPOINT ["/processor"]
