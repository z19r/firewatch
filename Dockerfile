FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /firewatch . && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /firewatch /firewatch
# owned by nonroot so the web UI can save its config to a mounted volume
COPY --from=build --chown=65532:65532 /data /data
ENV CONFIG=/data/firewatch.yaml
VOLUME /data
ENTRYPOINT ["/firewatch"]
