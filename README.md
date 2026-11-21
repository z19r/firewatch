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

| Variable          | Required | Default | Meaning                                                            |
| ----------------- | -------- | ------- | ------------------------------------------------------------------ |
| `SENTRY_DSN`      | yes      |         | DSN of the Sentry project that receives the metrics                |
| `TARGETS`         | yes      |         | Prometheus endpoint URLs, separated by commas or whitespace        |
| `SCRAPE_INTERVAL` | no       | `1m`    | Go duration, e.g. `30s`                                            |
| `METRIC_FILTER`   | no       |         | Regex; only metric families whose name matches are shipped         |

Sentry bills by metric volume, so start with a `METRIC_FILTER` (for example
`^(http_|process_)`) and a relaxed interval, then widen.

## How metrics are mapped

Sentry has counters, gauges and distributions. Prometheus series map like this:

| Prometheus                     | Sentry                                                                 |
| ------------------------------ | ---------------------------------------------------------------------- |
| counter                        | counter, sent as the increase since the previous scrape                |
| gauge, untyped                 | gauge                                                                  |
| histogram                      | counters `<name>_count`, `<name>_sum`, `<name>_bucket` (with `le`), as increases |
| summary                        | counters `<name>_count`, `<name>_sum`; quantiles as gauges with `quantile` |
| (synthetic) `up`               | gauge, `1` if the scrape worked, `0` if not                            |

- Labels become attributes. Every metric also gets `instance` (the target's
  `host:port`).
- Names ending in `_seconds` or `_bytes` (optionally `_total`) get the unit
  `second` or `byte`.
- The first scrape of a target only records a baseline for cumulative series,
  so a restart never replays a counter's lifetime total. A counter that goes
  backwards is treated as reset.
- Counters that did not change are not sent. `NaN` and `±Inf` samples are
  skipped.

## Limits

- A batch Sentry rejects (outage, rate limit) is logged and dropped, not
  retried.
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
