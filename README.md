# Satellite TLE Catalog Mirror

This standalone project mirrors public Space-Track GP/TLE data into a static catalog that the iOS app can download without exposing Space-Track credentials to users.

## What It Does

- Logs in to Space-Track from a private GitHub Actions runner.
- Fetches the latest propagable GP catalog no more than once per hour.
- Uses the optimized hourly delta query when a prior catalog exists.
- Merges updates by NORAD catalog ID and keeps the newest TLE for each object.
- Publishes `manifest.json`, `current.3le`, and `current.3le.gz` to Cloudflare R2.
- Builds server-assisted position snapshots from the mirrored TLE catalog.
- Publishes `snapshots/manifest.json`, `snapshots/current.json`, and `snapshots/current.json.gz`.
- Builds Space-Track-only metadata insights from `gp`, `satcat`, `decay`, and optional `satcat_debut`.
- Publishes `insights/manifest.json`, `insights/current.json`, `insights/current.json.gz`, plus **`insights/history.json`** (append-only trend snapshots) and **`insights/history.json.gz`**.
- Serves the files through a tiny Cloudflare Worker.

## Required Secrets

Add these to the GitHub repo that owns this mirror:

- `SPACE_TRACK_IDENTITY`
- `SPACE_TRACK_PASSWORD`
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- `PUBLIC_CATALOG_BASE_URL`
- `PUBLIC_SNAPSHOT_BASE_URL`
- `PUBLIC_INSIGHTS_BASE_URL` optional; defaults to the Worker route ending in `/insights` when omitted.

`PUBLIC_CATALOG_BASE_URL` should be the public Worker route ending in `/catalog`, for example:

```text
https://satellite-tle-catalog.<your-subdomain>.workers.dev/catalog
```

`PUBLIC_SNAPSHOT_BASE_URL` should be the public Worker route ending in `/snapshots`, for example:

```text
https://satellite-tle-catalog.<your-subdomain>.workers.dev/snapshots
```

## One-Time Cloudflare Setup

1. Create an R2 bucket, for example `satellite-tle-catalog`.
2. Create an R2 API token with read/write access for that bucket.
3. Deploy the Worker in `worker/` with Wrangler.
4. Copy the Worker `/catalog` URL into the iOS app's hosted catalog setting.
5. Copy the Worker `/snapshots/current.json` URL into the app snapshot client once the feature flag is ready.

## Local Test

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests
```

For a dry run with real Space-Track credentials and no R2 upload:

```sh
SPACE_TRACK_IDENTITY="you@example.com" \
SPACE_TRACK_PASSWORD="..." \
python scripts/mirror_spacetrack.py --dry-run --output-dir build/catalog
```

For a local snapshot build from an existing catalog file:

```sh
python scripts/build_snapshot.py --catalog-file build/catalog/current.3le --dry-run --output-dir build/snapshots
```

For a local Space-Track insights build with real credentials and no R2 upload:

```sh
SPACE_TRACK_IDENTITY="you@example.com" \
SPACE_TRACK_PASSWORD="..." \
python scripts/build_insights.py --dry-run --output-dir public/insights
```

The insights pipeline caches raw Space-Track JSON under `build/insights/cache` by default and reuses it for 23 hours unless `--force-refresh` is passed. The live app-facing artifact is **`public/insights/current.json`** (today, upcoming, highlights, breakdowns). Each successful run also downloads the prior **`history.json`** from R2 (if any), appends one snapshot, trims to **`INSIGHTS_HISTORY_MAX_SNAPSHOTS`** (default 2500), and writes **`public/insights/history.json`** for trend charts (launches/reentries today counts, active vs debris, orbit bands, constellation counts). Dry runs merge against a local `history.json` in the output folder when present.

Do not commit credentials.
