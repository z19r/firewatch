host := "root@z19r-networking"

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

# Test, then cross-compile for z19r-networking (arm64) and restart its systemd service
deploy: lint test
    @git diff --quiet HEAD || { echo "uncommitted changes: commit before deploying"; exit 1; }
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o firewatch-arm64 .
    tar -cz firewatch-arm64 | ssh {{host}} 'cd /tmp && tar xzm && sudo install -m 0755 firewatch-arm64 /usr/local/bin/firewatch && rm firewatch-arm64 && sudo systemctl restart firewatch && sleep 2 && sudo systemctl is-active firewatch'
    rm firewatch-arm64

# Install or update the systemd unit on the host (config and /etc/firewatch.env stay as they are)
service:
    ssh {{host}} 'cat > /etc/systemd/system/firewatch.service && rm -rf /etc/systemd/system/firewatch.service.d && systemctl daemon-reload && systemctl enable firewatch' < deploy/firewatch.service

# Tail the production logs
logs:
    ssh {{host}} sudo journalctl -u firewatch -f -o cat

clean:
    rm -f firewatch firewatch-arm64
