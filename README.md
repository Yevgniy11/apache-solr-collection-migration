# Apache Solr Collection Migration

Migrates documents from a source Solr collection to a destination Solr collection
using cursor-based pagination for reliable, complete migration.

The purpose of this script is to enable a relatively fast migration from a source
Solr to a destination Solr without invoking a full reindexing of the system,
because Solr does not natively support persistent data migration between
different collections.

## Prerequisites

- The destination collection **must already exist** with a schema that matches the
  source collection. This script migrates documents only — it does not create
  collections or replicate schema configuration.
- Both Solr instances must be reachable over HTTP(S) from the machine running the
  script.

## Requirements

- Python 3.7+
- `requests` library

```bash
pip install requests
```

## Usage

```bash
python migrate_solr_collection.py <source_url> <destination_url> [options]
```

## Arguments

| Argument            | Description                                                        |
|---------------------|--------------------------------------------------------------------|
| `source_url`        | Full URL to source collection (e.g. `http://host:8983/solr/collection_name/`) |
| `destination_url`   | Full URL to destination collection (same format)                   |

## Options

| Option          | Description                                                              |
|-----------------|--------------------------------------------------------------------------|
| `-b`, `--batch-size N` | Documents per fetch (default: 20000)                               |
| `-t`, `--timeout SEC`  | HTTP timeout in seconds (default: 200)                             |
| `--resume`             | Continue from where a previous run left off (skips by max ID)      |
| `--reconcile`          | Find and re-index documents missing from destination (compares all IDs) |
| `--no-commit`          | Use `commitWithin` instead of explicit commits after each batch    |
| `-k`, `--insecure`     | Disable SSL certificate verification (for self-signed certs)       |

> **Note:** `--resume` and `--reconcile` are mutually exclusive.

## Examples

Basic migration:

```bash
python migrate_solr_collection.py http://localhost:8983/solr/my_collection/ http://localhost:8983/solr/my_collection_v2/
```

Migration with custom batch size and SSL verification disabled:

```bash
python migrate_solr_collection.py http://solr:8983/solr/old/ http://solr:8983/solr/new/ --batch-size 20000 --insecure
```

Resume an interrupted migration:

```bash
python migrate_solr_collection.py http://solr:8983/solr/old/ http://solr:8983/solr/new/ --resume
```

Reconcile missing documents after a migration:

```bash
python migrate_solr_collection.py http://solr:8983/solr/old/ http://solr:8983/solr/new/ --reconcile
```

## How It Works

### Full Migration (default)

Documents are fetched from the source collection in batches using Solr's
**cursorMark** pagination, cleaned of internal fields (`_version_`, `_root_`,
`_text_`), and posted to the destination. Fetching and uploading are pipelined
so the next batch is fetched while the current one is being uploaded.

### Resume (`--resume`)

Looks up the highest document ID already present in the destination and resumes
fetching from the source starting after that ID. Useful when a previous run was
interrupted.

### Reconcile (`--reconcile`)

Streams sorted document IDs from both source and destination, performs a merge
comparison to find IDs present in the source but missing from the destination,
then fetches and re-indexes only the missing documents. This is the safest way
to ensure the destination is a complete copy.

## Benchmark

### Medium collection (~2.1M documents)

Sample run with the default batch size of 20,000:

| Metric              | Value          |
|---------------------|----------------|
| Total documents     | 2,114,783      |
| Batch size          | 20,000         |
| Batches             | 106            |
| Total time          | 195.85s (~3.3 min) |
| Throughput          | ~10,798 docs/s |
| Avg fetch per batch | ~1.7s          |
| Avg upload per batch| ~1.4s          |

<details>
<summary>Abbreviated log output</summary>

```
2026-02-17 10:47:45 [INFO] Source: 2114783 documents | batch_size=20000
2026-02-17 10:47:47 [INFO] Batch 1: 20000 docs (total: 20000 / 2114783) | fetch: 2.41s | clean: 0.02s
2026-02-17 10:47:50 [INFO] Batch 1 upload: 2.14s
2026-02-17 10:47:50 [INFO] Batch 2: 20000 docs (total: 40000 / 2114783) | fetch: 2.43s | clean: 0.02s
2026-02-17 10:47:52 [INFO] Batch 2 upload: 1.23s
...
2026-02-17 10:49:19 [INFO] Batch 50: 20000 docs (total: 1000000 / 2114783) | fetch: 1.55s | clean: 0.03s
2026-02-17 10:49:19 [INFO] Batch 50 upload: 1.26s
...
2026-02-17 10:50:48 [INFO] Batch 100: 20000 docs (total: 2000000 / 2114783) | fetch: 1.57s | clean: 0.03s
2026-02-17 10:50:48 [INFO] Batch 100 upload: 1.98s
...
2026-02-17 10:50:59 [INFO] Batch 106: 14783 docs (total: 2114783 / 2114783) | fetch: 1.17s | clean: 0.02s
2026-02-17 10:51:01 [INFO] Batch 106 upload: 1.43s
2026-02-17 10:51:01 [INFO] Done. 2114783 documents | 195.85s | 10798.0 docs/s
```

</details>

### Large collection (~13M documents)

Sample run with the default batch size of 20,000:

| Metric              | Value              |
|---------------------|--------------------|
| Total documents     | 12,996,161         |
| Batch size          | 20,000             |
| Batches             | ~650               |
| Avg fetch per batch | ~2.4s              |
| Avg upload per batch| ~1.3s              |
| Observed throughput | ~8,125 docs/s      |
| Estimated total time| ~27 min            |

<details>
<summary>Abbreviated log output</summary>

```
2026-02-17 12:41:54 [INFO] Source: 12996161 documents | batch_size=20000
2026-02-17 12:41:56 [INFO] Batch 1: 20000 docs (total: 20000 / 12996161) | fetch: 2.49s | clean: 0.02s
2026-02-17 12:41:59 [INFO] Batch 1 upload: 1.38s
2026-02-17 12:41:59 [INFO] Batch 2: 20000 docs (total: 40000 / 12996161) | fetch: 2.63s | clean: 0.02s
2026-02-17 12:42:01 [INFO] Batch 2 upload: 1.36s
...
2026-02-17 12:42:56 [INFO] Batch 25: 20000 docs (total: 500000 / 12996161) | fetch: 2.41s | clean: 0.03s
2026-02-17 12:42:59 [INFO] Batch 25 upload: 1.26s
...
2026-02-17 12:43:28 [INFO] Batch 38: 20000 docs (total: 760000 / 12996161) | fetch: 2.46s | clean: 0.03s
2026-02-17 12:43:30 [INFO] Batch 38 upload: 1.26s
2026-02-17 12:43:30 [INFO] Batch 39: 20000 docs (total: 780000 / 12996161) | fetch: 2.25s | clean: 0.02s
...
```

</details>

> **Note:** Actual throughput depends on network latency, document size, Solr
> configuration, and hardware. The pipelined fetch/upload design keeps both
> source and destination busy, maximizing throughput.
