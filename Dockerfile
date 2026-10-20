# Build a static binary, then ship it with the fixture feed in a small non-root image.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/server /app/server
COPY fixtures /app/fixtures
USER app
EXPOSE 8080
ENTRYPOINT ["/app/server"]
