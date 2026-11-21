pi := "dietpi@10.0.1.57"

default:
    @just --list

# Run the tests with the race detector
test:
    go test -race ./...

# gofmt + go vet
lint:
    test -z "$(gofmt -l .)"
    go vet ./...

# Build a local binary
build:
    go build -o firewatch .

# Run locally with the web UI and /metrics on loopback
run: build
    LISTEN=127.0.0.1:8080 METRICS_LISTEN=127.0.0.1:9091 ./firewatch

# Build the Docker image
docker:
    docker build -t firewatch .

# Cross-compile for the Pi and restart its systemd service (no scp there: tar over ssh)
deploy:
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o firewatch-arm64 .
    tar -cz firewatch-arm64 | ssh {{pi}} 'cd /tmp && tar xzm && sudo install -m 0755 firewatch-arm64 /usr/local/bin/firewatch && rm firewatch-arm64 && sudo systemctl restart firewatch && sleep 2 && sudo systemctl is-active firewatch'
    rm firewatch-arm64

# Tail the Pi's logs
logs:
    ssh {{pi}} sudo journalctl -u firewatch -f -o cat

clean:
    rm -f firewatch firewatch-arm64
