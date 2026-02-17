#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Solr Collection Migration Script

Migrates documents from a source Solr collection to a destination Solr collection
using cursor-based pagination for reliable, complete migration.

Usage:
    python migrate_solr_collection.py <source_url> <destination_url> [--batch-size N]

Examples:
    python migrate_solr_collection.py http://localhost:8983/solr/term_search/ http://localhost:8983/solr/term_search_new/
    python migrate_solr_collection.py http://solr:8983/solr/old/ http://solr:8983/solr/new/ --batch-size 20000 --insecure

Arguments:
    source_url      Full URL to source collection (e.g. http://host:8983/solr/collection_name/)
    destination_url Full URL to destination collection (same format)
    --batch-size    Documents per fetch (default: 20000)
    --resume        Continue from where a previous run left off (skips by max ID)
    --reconcile     Find and re-index documents missing from destination (compares all IDs)
    --no-commit     Do not issue explicit commit after each batch
    --insecure      Disable SSL certificate verification (for self-signed certs)
"""

import argparse
import json
import logging
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FIELDS_TO_STRIP = frozenset({"_version_", "_root_", "_text_"})
REQUEST_TIMEOUT = 200
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------


def _normalize_url(url: str) -> str:
    """Ensure collection URL has a trailing slash."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") + "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _select_url(base: str) -> str:
    return urljoin(base, "select")


def _update_url(base: str, commit: bool) -> str:
    handler = urljoin(base, "update")
    if commit:
        return f"{handler}?commit=true&commitWithin=0&overwrite=true"
    return f"{handler}?commitWithin=5000&overwrite=true"


# ---------------------------------------------------------------------------
# Low-level Solr operations
# ---------------------------------------------------------------------------


def _retry(fn, retries: int = RETRY_ATTEMPTS, context: str = ""):
    """Call *fn* with exponential-backoff retries on transient errors."""
    for attempt in range(retries):
        try:
            return fn()
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            if attempt == retries - 1:
                raise
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning(
                "%s (attempt %d/%d): %s — retrying in %ds",
                context,
                attempt + 1,
                retries,
                exc,
                delay,
            )
            time.sleep(delay)


def _clean(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Strip internal Solr fields before re-indexing."""
    return {k: v for k, v in doc.items() if k not in FIELDS_TO_STRIP}


def _get_total(source_url: str, timeout: int, verify: bool) -> int:
    """Return total document count in the source collection."""
    url = _select_url(source_url)
    params = {"q": "*:*", "rows": 0, "wt": "json"}
    resp = _retry(
        lambda: requests.get(url, params=params, timeout=timeout, verify=verify),
        context="Count",
    )
    return resp.json().get("response", {}).get("numFound", 0)


def _get_max_id(collection_url: str, timeout: int, verify: bool) -> Optional[str]:
    """Return the max document ID in a collection, or None if empty."""
    url = _select_url(collection_url)
    params = {"q": "*:*", "sort": "id desc", "rows": 1, "fl": "id", "wt": "json"}
    resp = _retry(
        lambda: requests.get(url, params=params, timeout=timeout, verify=verify),
        context="MaxID",
    )
    docs = resp.json().get("response", {}).get("docs", [])
    if docs:
        return docs[0].get("id")
    return None


def _stream_ids(
    collection_url: str, batch_size: int, timeout: int, verify: bool
):
    """Yield document IDs in sorted order using lightweight cursor pagination."""
    url = _select_url(collection_url)
    cursor_mark = "*"
    while True:
        params = {
            "q": "*:*",
            "sort": "id asc",
            "rows": batch_size,
            "cursorMark": cursor_mark,
            "wt": "json",
            "fl": "id",
        }
        resp = _retry(
            lambda: requests.get(url, params=params, timeout=timeout, verify=verify),
            context="StreamIDs",
        )
        data = resp.json()
        docs = data.get("response", {}).get("docs", [])
        for doc in docs:
            yield doc["id"]
        next_cursor = data.get("nextCursorMark")
        if not docs or next_cursor is None or next_cursor == cursor_mark:
            break
        cursor_mark = next_cursor


def _fetch_by_ids(
    source_url: str,
    ids: List[str],
    timeout: int,
    verify: bool,
) -> List[Dict[str, Any]]:
    """Fetch full documents from source for a list of IDs (POST to avoid URL length limits)."""
    if not ids:
        return []
    url = _select_url(source_url)
    fq = "{!terms f=id}" + ",".join(ids)
    data = {"q": "*:*", "fq": fq, "rows": len(ids), "wt": "json", "fl": "*"}
    resp = _retry(
        lambda: requests.post(url, data=data, timeout=timeout, verify=verify),
        context="FetchByIDs",
    )
    return [_clean(d) for d in resp.json().get("response", {}).get("docs", [])]


def _fetch(
    source_url: str, cursor_mark: str, rows: int, timeout: int, verify: bool,
    fq: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch one batch via cursorMark.  O(rows) regardless of collection depth.
    Returns (docs, next_cursor).  *next_cursor* equals *cursor_mark* when exhausted.
    """
    url = _select_url(source_url)
    params = {
        "q": "*:*",
        "sort": "id asc",
        "rows": rows,
        "cursorMark": cursor_mark,
        "wt": "json",
        "fl": "*",
    }
    if fq:
        params["fq"] = fq
    resp = _retry(
        lambda: requests.get(url, params=params, timeout=timeout, verify=verify),
        context="Fetch",
    )
    data = resp.json()
    docs = data.get("response", {}).get("docs", [])
    return docs, data.get("nextCursorMark")


def _post(
    dest_url: str, docs: List[Dict[str, Any]], commit: bool, timeout: int, verify: bool
) -> None:
    """Post a batch of already-cleaned documents to the destination."""
    if not docs:
        return
    url = _update_url(dest_url, commit)
    headers = {"Content-Type": "application/json", "Accept": "text/plain"}
    payload = json.dumps(docs)
    resp = _retry(
        lambda: requests.post(
            url, data=payload, headers=headers, timeout=timeout, verify=verify
        ),
        context="Upload",
    )
    result = resp.json()
    status = result.get("responseHeader", {}).get("status", 0)
    if status != 0:
        raise RuntimeError(f"Solr update error (status {status}): {result}")


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------


def _timed_post(
    dest_url: str, docs: List[Dict[str, Any]], commit: bool, timeout: int, verify: bool
) -> float:
    """Post documents and return elapsed seconds."""
    t0 = time.perf_counter()
    _post(dest_url, docs, commit, timeout, verify)
    return time.perf_counter() - t0


def _wait_and_log_upload(prev: Optional[Future], prev_batch_num: int) -> None:
    """Wait for the previous upload future and log its upload time."""
    if prev is None:
        return
    upload_time = prev.result()
    logger.info("Batch %d upload: %.2fs", prev_batch_num, upload_time)


def run_migration(
    source_url: str,
    destination_url: str,
    batch_size: int = 20000,
    commit: bool = True,
    resume: bool = False,
    timeout: int = REQUEST_TIMEOUT,
    verify: bool = True,
) -> int:
    """
    Migrate all documents using cursor-based pagination.

    Each batch: fetch → clean → submit upload (async).
    The upload runs in the background while the next batch is being fetched.
    When *resume* is True, skips documents already present in the destination.
    """
    source_url = _normalize_url(source_url)
    destination_url = _normalize_url(destination_url)

    start_time = time.perf_counter()
    total = _get_total(source_url, timeout, verify)
    logger.info("Source: %d documents | batch_size=%d", total, batch_size)

    # Resume: skip documents already in destination
    fq: Optional[str] = None
    already_migrated = 0
    if resume:
        dest_count = _get_total(destination_url, timeout, verify)
        max_id = _get_max_id(destination_url, timeout, verify)
        if max_id:
            fq = 'id:{%s TO *}' % max_id
            already_migrated = dest_count
            logger.info("Resuming after id=%s (%d docs already in destination)", max_id, dest_count)
        else:
            logger.info("Destination is empty, starting fresh")

    total_migrated = already_migrated
    cursor_mark = "*"
    batch_num = 0
    prev_future: Optional[Future] = None
    prev_batch_num = 0

    with ThreadPoolExecutor(max_workers=1) as executor:
        while True:
            # 1. Fetch
            t0 = time.perf_counter()
            docs, next_cursor = _fetch(
                source_url, cursor_mark, batch_size, timeout, verify, fq=fq
            )
            batch_fetch = time.perf_counter() - t0
            if not docs:
                break
            batch_num += 1

            # 2. Clean
            t0 = time.perf_counter()
            cleaned = [_clean(d) for d in docs]
            batch_clean = time.perf_counter() - t0

            # Wait for previous upload to finish before submitting next
            _wait_and_log_upload(prev_future, prev_batch_num)

            # 3. Upload (async — overlaps with next fetch)
            prev_future = executor.submit(
                _timed_post, destination_url, cleaned, commit, timeout, verify
            )
            prev_batch_num = batch_num

            total_migrated += len(docs)
            logger.info(
                "Batch %d: %d docs (total: %d / %d) | fetch: %.2fs | clean: %.2fs",
                batch_num,
                len(docs),
                total_migrated,
                total,
                batch_fetch,
                batch_clean,
            )

            if (
                next_cursor is None
                or next_cursor == cursor_mark
                or len(docs) < batch_size
            ):
                break
            cursor_mark = next_cursor

        # Wait for the last upload
        _wait_and_log_upload(prev_future, prev_batch_num)

    elapsed = time.perf_counter() - start_time
    throughput = total_migrated / elapsed if elapsed > 0 else 0
    logger.info(
        "Done. %d documents | %.2fs | %.1f docs/s", total_migrated, elapsed, throughput
    )
    return total_migrated


def run_reconciliation(
    source_url: str,
    destination_url: str,
    batch_size: int = 20000,
    commit: bool = True,
    timeout: int = REQUEST_TIMEOUT,
    verify: bool = True,
) -> int:
    """
    Find documents present in source but missing from destination and re-index them.

    Streams sorted IDs from both collections and performs a merge comparison.
    Only the missing documents are fetched (in full) and posted.
    """
    source_url = _normalize_url(source_url)
    destination_url = _normalize_url(destination_url)

    start_time = time.perf_counter()
    src_total = _get_total(source_url, timeout, verify)
    dst_total = _get_total(destination_url, timeout, verify)
    logger.info(
        "Reconcile: source=%d docs, destination=%d docs, diff=%d",
        src_total, dst_total, src_total - dst_total,
    )

    src_ids: Iterator[str] = _stream_ids(source_url, batch_size, timeout, verify)
    dst_ids: Iterator[str] = _stream_ids(destination_url, batch_size, timeout, verify)

    src_id = next(src_ids, None)
    dst_id = next(dst_ids, None)
    compared = 0
    missing: List[str] = []
    total_reindexed = 0
    reconcile_batch_size = 1000

    while src_id is not None:
        if dst_id is None or src_id < dst_id:
            missing.append(src_id)
            src_id = next(src_ids, None)
        elif src_id == dst_id:
            src_id = next(src_ids, None)
            dst_id = next(dst_ids, None)
        else:
            dst_id = next(dst_ids, None)

        compared += 1
        if compared % 500_000 == 0:
            logger.info("Compared %d IDs, %d missing so far ...", compared, len(missing) + total_reindexed)

        if len(missing) >= reconcile_batch_size:
            docs = _fetch_by_ids(source_url, missing, timeout, verify)
            _post(destination_url, docs, commit, timeout, verify)
            total_reindexed += len(docs)
            logger.info("Re-indexed batch of %d missing docs (total re-indexed: %d)", len(docs), total_reindexed)
            missing.clear()

    # Flush remaining missing IDs
    if missing:
        docs = _fetch_by_ids(source_url, missing, timeout, verify)
        _post(destination_url, docs, commit, timeout, verify)
        total_reindexed += len(docs)
        logger.info("Re-indexed batch of %d missing docs (total re-indexed: %d)", len(docs), total_reindexed)

    elapsed = time.perf_counter() - start_time
    logger.info("Reconcile done. %d missing docs re-indexed | %.2fs", total_reindexed, elapsed)
    return total_reindexed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate documents between Solr collections using cursor-based pagination.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("source", help="Source collection URL")
    parser.add_argument("destination", help="Destination collection URL")
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=20000,
        metavar="N",
        help="Documents per fetch (default: 20000)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=REQUEST_TIMEOUT,
        metavar="SEC",
        help="HTTP timeout in seconds (default: 200)",
    )
    parser.add_argument(
        "-k",
        "--insecure",
        action="store_true",
        help="Skip SSL certificate verification",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--resume",
        action="store_true",
        help="Continue from where a previous run left off (skips by max ID)",
    )
    mode_group.add_argument(
        "--reconcile",
        action="store_true",
        help="Find and re-index documents missing from destination (compares all IDs)",
    )
    parser.add_argument(
        "--no-commit",
        action="store_true",
        dest="no_commit",
        help="Use commitWithin instead of explicit commits",
    )

    args = parser.parse_args()

    if args.batch_size < 1:
        logger.error("--batch-size must be >= 1")
        return 2

    try:
        if args.reconcile:
            run_reconciliation(
                source_url=args.source,
                destination_url=args.destination,
                batch_size=args.batch_size,
                commit=not args.no_commit,
                timeout=args.timeout,
                verify=not args.insecure,
            )
        else:
            run_migration(
                source_url=args.source,
                destination_url=args.destination,
                batch_size=args.batch_size,
                commit=not args.no_commit,
                resume=args.resume,
                timeout=args.timeout,
                verify=not args.insecure,
            )
        return 0
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 130
    except Exception as exc:
        logger.exception("Migration failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
