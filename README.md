# firewatch

Prometheus brought the fire. Sentry keeps watch.

`firewatch` scrapes Prometheus `/metrics` endpoints and ships the samples to
[Sentry Application Metrics](https://docs.sentry.io/product/metrics/). One
static binary, no config file, no state on disk.

## Run it

```sh
docker run --rm \
  -e SENTRY_DSN='https://<key>@o<org>.ingest.sentry.io/<project>' \
  -e TARGETS='http://node-exporter:9100/metrics,http://api:8080/metrics' \
  ghcr.io/z19r/firewatch
```

Or without Docker:

```sh
go install github.com/z19r/firewatch@latest
SENTRY_DSN=... TARGETS=http://localhost:9100/metrics firewatch
```

## Configuration

There are three ways to configure it. Pick one, or mix YAML and the web UI.

### Environment only

Every target ships to the same Sentry project. Used when there is no YAML file.

| Variable          | Required | Default | Meaning                                                     |
| ----------------- | -------- | ------- | ----------------------------------------------------------- |
| `SENTRY_DSN`      | yes      |         | DSN of the Sentry project that receives the metrics         |
| `TARGETS`         | yes      |         | Prometheus endpoint URLs, separated by commas or whitespace |
| `SCRAPE_INTERVAL` | no       | `1m`    | Go duration, e.g. `30s`                                     |
| `METRIC_FILTER`   | no       |         | Regex; only metric families whose name matches are shipped  |

### YAML

`firewatch.yaml` in the working directory, or wherever `CONFIG` points (the
Docker image uses `/data/firewatch.yaml`). When
the file exists it replaces the environment variables above. Each connection
pairs one Prometheus endpoint with one Sentry project:

```yaml
connections:
  - target: http://node-exporter:9100/metrics
    dsn: https://<key>@o<org>.ingest.sentry.io/<project>
    interval: 30s # optional, default 1m
    filter: ^node_(cpu|memory)_ # optional regex on metric names
  - target: http://api:8080/metrics
    dsn: https://<key>@o<org>.ingest.sentry.io/<other-project>
```

The file is read at startup. It holds DSNs, so keep it private.

### Web UI (optional)

Set `LISTEN` to turn on a small page that lists the connections with their
last scrape status and lets you add, edit and delete them. Changes take effect
immediately and are written back to the YAML file (comments in the file are
not preserved). If the list changed since you loaded the page, an edit is
refused and the page shows the current list.

```sh
docker run --rm -p 8080:8080 -v firewatch:/data \
  -e LISTEN=:8080 -e UI_PASSWORD=change-me \
  ghcr.io/z19r/firewatch
```

| Variable      | Meaning                                                                                |
| ------------- | -------------------------------------------------------------------------------------- |
| `LISTEN`      | Address for the UI, e.g. `:8080` or `127.0.0.1:8080`. Unset means no UI.               |
| `UI_PASSWORD` | HTTP basic-auth password (any username). Required unless `LISTEN` is loopback.         |
| `CONFIG`      | Path of the YAML file. Default `firewatch.yaml` (`/data/firewatch.yaml` in the image). |

The UI serves plain HTTP. Put it behind a TLS-terminating proxy if it leaves a
trusted network: whoever can log in can read the DSNs and make firewatch fetch
any URL it can reach.

Sentry bills by metric volume, so start with a filter (for example
`^(http_|process_)`) and a relaxed interval, then widen.

## How metrics are mapped

Sentry has counters, gauges and distributions. Prometheus series map like this:

| Prometheus       | Sentry                                                                           |
| ---------------- | -------------------------------------------------------------------------------- |
| counter          | counter, sent as the increase since the previous scrape                          |
| gauge, untyped   | gauge                                                                            |
| histogram        | counters `<name>_count`, `<name>_sum`, `<name>_bucket` (with `le`), as increases |
| summary          | counters `<name>_count`, `<name>_sum`; quantiles as gauges with `quantile`       |
| (synthetic) `up` | gauge, `1` if the scrape worked, `0` if not                                      |

- Labels become attributes. Every metric also gets `instance` (the target's
  `host:port`, plus the path when two targets share a `host:port`).
- Names ending in `_seconds` or `_bytes` (optionally `_total`) get the unit
  `second` or `byte`.
- The first scrape of a target only records a baseline for cumulative series,
  so a restart never replays a counter's lifetime total. A counter that goes
  backwards is treated as reset. A series missing from up to 5 scrapes in a
  row is still sent as a delta when it comes back.
- Counters that did not change are not sent. `NaN` and `±Inf` samples are
  skipped.

## Limits

- A batch Sentry rejects (outage) is logged and dropped, not retried; the
  other batches of that scrape are still sent. On a rate limit (429) the rest
  of that scrape is dropped.
- Targets are scraped with a plain GET: no auth headers, no custom TLS, no
  service discovery.
- Native histograms and Sentry distributions are not used: an already
  aggregated histogram cannot be turned back into individual samples.

## Develop

```sh
go test -race ./...
docker build -t firewatch .
```

MIT licensed.
