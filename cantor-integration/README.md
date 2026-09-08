# cantor-integration

The `cantor-integration` module contains everything needed to run the integration
test harness. 

## Files

- `BenchmarkStats`: sorts the per-method timing samples from one backend/event-count run and computes the summary metrics (count, sum, avg, min, max, and P50/P90/P95/P99 percentiles)
- `CSVReporter` : writes a list of `BenchmarkStats` to its corresponding CSV file
- `HTMLReporter` : reads the CSV files in `reports/` and renders them all into a single HTML report
- `IntegrationTestRunner` : the main entry point; connects to the running Cantor server, runs the tests, saves the stats to CSV, and regenerates the HTML report
- `ServerManager` : opens and holds the gRPC client connection to the Cantor server
- `TimingListener` : a TestNG `ITestListener` that records each test method's duration and pass/fail/skip status
- `IntegrationEventsTest` : holds the test cases; it stores the events once, waits until they're queryable, then runs the test cases below and asserts the expected result count:
    - `getAll` : retrieves all stored events
    - `getMetadata` : gets the value of the metadata key `metadata-key-0` for each event
    - `getDimension` : queries events that have the dimension key `dimension-key-0`
    - `getMetadataExactMatch` : queries events whose metadata key `user` has the value `user-0`
    - `getMetadataPatternMatch` : queries events whose metadata key `region` has a value beginning with `us-west-1`
    - `getDimensionExactMatch` : queries events whose dimension key `latency` has the value `0`
    - `getDimensionRangeMatch` : queries events whose dimension key `latency` falls within an inclusive range (`0..1`)

## Running Integration Tests

Integration tests spin up a real Cantor server (backed by H2, MySQL, S3, or
Multicloudj) in Docker, run the test suite against it, and tear everything
down at the end.

Run from the repository root:

```bash
./integration-test.sh --type <TYPE>
```

where `<TYPE>` is one of:

| Type                 | Storage     |
|----------------------|-------------|
| `CantorOnH2`         | H2          |
| `CantorOnMySQL`      | MySQL       |
| `CantorOnS3`         | S3          |
| `CantorOnMulticloudj`| Multicloudj |

If `--type` is omitted, it defaults to `CantorOnH2`.

### Choosing the Select strategy (CantorOnS3 only)

For `CantorOnS3`, use `--select` to choose between
`s3` for S3 Select (server-side) and `local` for Local Select (client-side).
It defaults to `s3` and is ignored for H2 and MySQL.

```bash
./integration-test.sh --type CantorOnS3 --select s3
./integration-test.sh --type CantorOnS3 --select local
```

### Choosing the cloud provider (CantorOnMulticloudj only)

For `CantorOnMulticloudj`, use `--provider` to choose the backing cloud:
`aws`, `gcp`, or `ali`. It defaults to `aws` and is ignored for other types.

```bash
./integration-test.sh --type CantorOnMulticloudj --provider aws
```

AWS and GCP fall back to their SDK-default credential chains (env vars,
instance profile, Application Default Credentials, etc.) when no static
credentials are configured. The `ali` provider has no such fallback and
requires static credentials to be supplied via `CANTOR_MULTICLOUDJ_ACCESS_KEY_ID`
and `CANTOR_MULTICLOUDJ_SECRET_ACCESS_KEY` (and optionally
`CANTOR_MULTICLOUDJ_SESSION_TOKEN` for temporary/STS credentials):

```bash
export CANTOR_MULTICLOUDJ_ACCESS_KEY_ID=<your-ali-access-key-id>
export CANTOR_MULTICLOUDJ_SECRET_ACCESS_KEY=<your-ali-secret-access-key>
./integration-test.sh --type CantorOnMulticloudj --provider ali
```

### Available flags

| Flag             | Description                                                                             |
|------------------|------------------------------------------------------------------------------------------|
| `-t, --type`     | Storage backend: `CantorOnH2`, `CantorOnMySQL`, `CantorOnS3`, or `CantorOnMulticloudj` (default `CantorOnH2`) |
| `-s, --select`   | For CantorOnS3 only; `s3` (S3Select) or `local` (LocalSelect); default `s3`             |
| `-p, --provider` | For CantorOnMulticloudj only; `aws`, `gcp`, or `ali`; default `aws`                     |
| `-c, --config`   | Path to a `cantor-server.conf` file (default `env/dockers/cantor/cantor-server.conf`)   |
| `-h, --help`     | Show the full list of options and exit                                                  |

## Reports

Each run writes results to `cantor-integration/reports/`:

- `<TYPE>.csv` - per-run metrics (count, sum, avg, min, max, P50, P90, P95, P99)
- `cantor-performance-metric.html` - a combined report

The HTML report is regenerated on every run. To regenerate it manually from the
existing CSVs:

```bash
cd cantor-integration
mvn compile exec:java@html-report
```