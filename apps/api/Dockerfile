# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/graphx-api ./cmd/api

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/graphx-api /graphx-api
EXPOSE 8080
ENTRYPOINT ["/graphx-api"]
