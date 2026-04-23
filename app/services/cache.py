"""In-memory cache for FDP metadata and datasets with background refresh."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

from app.services.fdp_client import FDPClient, FDPError


logger = logging.getLogger(__name__)


@dataclass
class FDPCacheEntry:
    """Cached snapshot of one FDP and all its datasets."""

    fdp_dict: Dict[str, Any]
    datasets: List[Dict[str, Any]] = field(default_factory=list)
    last_updated: Optional[datetime] = None
    error: Optional[str] = None


class FDPCache:
    """Thread-safe, process-wide cache of FDPs and their datasets.

    Shared across all HTTP sessions. A daemon thread periodically re-fetches
    every entry in the background so request handlers never wait on remote I/O.
    """

    def __init__(self, app_config: Dict[str, Any]):
        self._entries: Dict[str, FDPCacheEntry] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._refresh_thread: Optional[threading.Thread] = None
        self._is_refreshing = False

        self._timeout = app_config.get('FDP_TIMEOUT', 30)
        self._verify_ssl = app_config.get('FDP_VERIFY_SSL', True)
        self._refresh_interval = app_config.get('CACHE_REFRESH_INTERVAL', 300)
        self._default_fdps = list(app_config.get('DEFAULT_FDPS', []))

    def _make_client(self) -> FDPClient:
        return FDPClient(timeout=self._timeout, verify_ssl=self._verify_ssl)

    def fetch_and_cache_fdp(self, uri: str) -> Optional[FDPCacheEntry]:
        """Fetch one FDP + all its datasets and store in cache.

        On failure, keeps any previously cached data and records the error.
        Returns the entry (new or existing) or None if no prior data and fetch failed.
        """
        client = self._make_client()

        try:
            fdp = client.fetch_fdp(uri)
            # Fetch this FDP's catalogs (and their datasets) concurrently.
            datasets = []

            def _fetch_catalog(catalog_uri: str):
                try:
                    return client.fetch_catalog_with_datasets(
                        catalog_uri, fdp.uri, fdp.title
                    )
                except FDPError as e:
                    logger.warning(
                        f"Failed to fetch catalog {catalog_uri} in {fdp.uri}: {e}"
                    )
                    return []

            if fdp.catalogs:
                max_workers = max(1, min(len(fdp.catalogs), 5))
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = [pool.submit(_fetch_catalog, c) for c in fdp.catalogs]
                    for f in as_completed(futures):
                        datasets.extend(f.result())

            entry = FDPCacheEntry(
                fdp_dict=fdp.to_dict(),
                datasets=[ds.to_dict() for ds in datasets],
                last_updated=datetime.utcnow(),
                error=None,
            )
            with self._lock:
                self._entries[uri] = entry
            logger.info(
                f"Cached FDP {uri}: {fdp.title} ({len(datasets)} datasets)"
            )
            return entry
        except FDPError as e:
            logger.warning(f"Failed to refresh FDP {uri}: {e}")
            with self._lock:
                existing = self._entries.get(uri)
                if existing is not None:
                    existing.error = str(e)
                    return existing
            return None
        except Exception as e:
            logger.exception(f"Unexpected error refreshing FDP {uri}: {e}")
            with self._lock:
                existing = self._entries.get(uri)
                if existing is not None:
                    existing.error = str(e)
                    return existing
            return None

    def remove_fdp(self, uri: str) -> None:
        with self._lock:
            self._entries.pop(uri, None)

    def get_fdp(self, uri: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            entry = self._entries.get(uri)
            return entry.fdp_dict if entry else None

    def get_all_fdp_dicts(self) -> Dict[str, Dict[str, Any]]:
        """Return {uri: fdp_dict} for every cached FDP (copy)."""
        with self._lock:
            return {uri: e.fdp_dict for uri, e in self._entries.items()}

    def get_datasets_for_fdps(self, fdp_uris: List[str]) -> List[Dict[str, Any]]:
        """Return all cached dataset dicts belonging to the given FDP URIs."""
        with self._lock:
            out: List[Dict[str, Any]] = []
            for uri in fdp_uris:
                entry = self._entries.get(uri)
                if entry:
                    out.extend(entry.datasets)
            return out

    def get_dataset_by_uri(self, dataset_uri: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            for entry in self._entries.values():
                for ds in entry.datasets:
                    if ds.get('uri') == dataset_uri:
                        return ds
        return None

    def get_cache_info(self) -> Dict[str, Any]:
        """Lightweight snapshot for template display."""
        with self._lock:
            last_updated_vals = [
                e.last_updated for e in self._entries.values() if e.last_updated
            ]
            oldest = min(last_updated_vals) if last_updated_vals else None
            newest = max(last_updated_vals) if last_updated_vals else None
            return {
                'fdp_count': len(self._entries),
                'dataset_count': sum(len(e.datasets) for e in self._entries.values()),
                'last_updated': newest,
                'oldest_update': oldest,
                'is_refreshing': self._is_refreshing,
                'errors': {
                    uri: e.error for uri, e in self._entries.items() if e.error
                },
            }

    def get_entry(self, uri: str) -> Optional[FDPCacheEntry]:
        with self._lock:
            return self._entries.get(uri)

    def populate_defaults(self) -> None:
        """Fetch all DEFAULT_FDPS concurrently (blocking)."""
        if not self._default_fdps:
            return
        logger.info(f"Populating cache with {len(self._default_fdps)} default FDP(s)")
        self._refresh_all(self._default_fdps)

    def _refresh_all(self, uris: List[str]) -> None:
        """Re-fetch every URI in parallel. Safe for concurrent callers."""
        if not uris:
            return
        self._is_refreshing = True
        try:
            max_workers = max(1, min(len(uris), 5))
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [pool.submit(self.fetch_and_cache_fdp, uri) for uri in uris]
                for f in as_completed(futures):
                    f.result()
        finally:
            self._is_refreshing = False

    def start_background_refresh(self) -> None:
        """Launch the daemon thread that periodically refreshes every cached FDP."""
        if self._refresh_thread is not None and self._refresh_thread.is_alive():
            return
        self._stop_event.clear()
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop, name='FDPCacheRefresh', daemon=True
        )
        self._refresh_thread.start()
        logger.info(
            f"Started FDP cache background refresh "
            f"(every {self._refresh_interval}s)"
        )

    def stop_background_refresh(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._refresh_thread is not None:
            self._refresh_thread.join(timeout=timeout)
            self._refresh_thread = None

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(timeout=self._refresh_interval):
            try:
                with self._lock:
                    uris = list(self._entries.keys())
                if uris:
                    logger.info(f"Background refresh: re-fetching {len(uris)} FDP(s)")
                    self._refresh_all(uris)
            except Exception:
                logger.exception("Error during background cache refresh")
