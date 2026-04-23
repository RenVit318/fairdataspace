"""Tests for the FDP cache."""

import time
from unittest.mock import MagicMock, patch

import pytest

from app.models import Dataset, FairDataPoint
from app.services.cache import FDPCache


def _make_fdp(uri: str, title: str = 'Test FDP', catalogs=None) -> FairDataPoint:
    return FairDataPoint(
        uri=uri,
        title=title,
        catalogs=catalogs if catalogs is not None else [f'{uri}/catalog/1'],
        status='active',
    )


def _make_dataset(uri: str, fdp_uri: str, title: str = 'DS') -> Dataset:
    return Dataset(
        uri=uri,
        title=title,
        catalog_uri=f'{fdp_uri}/catalog/1',
        fdp_uri=fdp_uri,
        fdp_title='Test FDP',
    )


def _install_mock_client(cache: FDPCache, fdp: FairDataPoint, datasets):
    """Patch cache._make_client so it returns a mock with the given behavior.

    Returns a context manager. Use with `with _install_mock_client(...):`.
    """
    client_mock = MagicMock()
    client_mock.fetch_fdp.return_value = fdp
    client_mock.fetch_catalog_with_datasets.return_value = datasets
    return patch.object(cache, '_make_client', return_value=client_mock)


@pytest.fixture
def cache_config():
    return {
        'FDP_TIMEOUT': 5,
        'FDP_VERIFY_SSL': False,
        'CACHE_REFRESH_INTERVAL': 0.1,
        'DEFAULT_FDPS': [],
    }


class TestFetchAndCache:
    def test_fetch_and_cache_stores_fdp_and_datasets(self, cache_config):
        cache = FDPCache(cache_config)
        fdp = _make_fdp('https://example.org/fdp')
        ds = _make_dataset('https://example.org/dataset/1', fdp.uri)

        with _install_mock_client(cache, fdp, [ds]):
            entry = cache.fetch_and_cache_fdp(fdp.uri)

        assert entry is not None
        assert entry.fdp_dict['uri'] == fdp.uri
        assert len(entry.datasets) == 1
        assert entry.datasets[0]['uri'] == ds.uri
        assert entry.last_updated is not None
        assert entry.error is None

    def test_fetch_fdp_called_once_per_refresh(self, cache_config):
        """Avoid the double-fetch regression (plan called this out explicitly)."""
        cache = FDPCache(cache_config)
        fdp = _make_fdp('https://example.org/fdp')
        client_mock = MagicMock()
        client_mock.fetch_fdp.return_value = fdp
        client_mock.fetch_catalog_with_datasets.return_value = []

        with patch.object(cache, '_make_client', return_value=client_mock):
            cache.fetch_and_cache_fdp(fdp.uri)

        assert client_mock.fetch_fdp.call_count == 1

    def test_failed_fetch_keeps_old_data(self, cache_config):
        from app.services.fdp_client import FDPConnectionError

        cache = FDPCache(cache_config)
        fdp = _make_fdp('https://example.org/fdp')
        ds = _make_dataset('https://example.org/dataset/1', fdp.uri)

        with _install_mock_client(cache, fdp, [ds]):
            cache.fetch_and_cache_fdp(fdp.uri)

        failing = MagicMock()
        failing.fetch_fdp.side_effect = FDPConnectionError('boom')
        with patch.object(cache, '_make_client', return_value=failing):
            entry = cache.fetch_and_cache_fdp(fdp.uri)

        assert entry is not None, 'previous data should be preserved'
        assert entry.error == 'boom'
        assert len(cache.get_datasets_for_fdps([fdp.uri])) == 1

    def test_first_fetch_failure_returns_none(self, cache_config):
        from app.services.fdp_client import FDPConnectionError

        cache = FDPCache(cache_config)
        failing = MagicMock()
        failing.fetch_fdp.side_effect = FDPConnectionError('boom')
        with patch.object(cache, '_make_client', return_value=failing):
            assert cache.fetch_and_cache_fdp('https://nope') is None
        assert cache.get_fdp('https://nope') is None

    def test_catalog_failure_does_not_fail_whole_fdp(self, cache_config):
        from app.services.fdp_client import FDPConnectionError

        cache = FDPCache(cache_config)
        fdp = _make_fdp(
            'https://example.org/fdp',
            catalogs=['https://example.org/cat/1', 'https://example.org/cat/2'],
        )
        ds = _make_dataset('https://example.org/ds/1', fdp.uri)

        client_mock = MagicMock()
        client_mock.fetch_fdp.return_value = fdp

        def _catalog(catalog_uri, fdp_uri, fdp_title):
            if catalog_uri.endswith('/2'):
                raise FDPConnectionError('cat2 down')
            return [ds]

        client_mock.fetch_catalog_with_datasets.side_effect = _catalog
        with patch.object(cache, '_make_client', return_value=client_mock):
            entry = cache.fetch_and_cache_fdp(fdp.uri)

        assert entry is not None
        assert entry.error is None  # FDP itself succeeded
        assert len(entry.datasets) == 1


class TestReadMethods:
    def test_get_datasets_for_fdps_scopes_to_requested_uris(self, cache_config):
        cache = FDPCache(cache_config)
        fdp_a = _make_fdp('https://a.org/fdp', 'A')
        fdp_b = _make_fdp('https://b.org/fdp', 'B')
        ds_a = _make_dataset('https://a.org/ds/1', fdp_a.uri)
        ds_b = _make_dataset('https://b.org/ds/1', fdp_b.uri)

        with _install_mock_client(cache, fdp_a, [ds_a]):
            cache.fetch_and_cache_fdp(fdp_a.uri)
        with _install_mock_client(cache, fdp_b, [ds_b]):
            cache.fetch_and_cache_fdp(fdp_b.uri)

        only_a = cache.get_datasets_for_fdps([fdp_a.uri])
        assert [d['uri'] for d in only_a] == [ds_a.uri]

        both = cache.get_datasets_for_fdps([fdp_a.uri, fdp_b.uri])
        assert {d['uri'] for d in both} == {ds_a.uri, ds_b.uri}

    def test_remove_fdp_drops_entry(self, cache_config):
        cache = FDPCache(cache_config)
        fdp = _make_fdp('https://example.org/fdp')
        with _install_mock_client(cache, fdp, []):
            cache.fetch_and_cache_fdp(fdp.uri)
        assert cache.get_fdp(fdp.uri) is not None
        cache.remove_fdp(fdp.uri)
        assert cache.get_fdp(fdp.uri) is None

    def test_get_cache_info_reports_counts(self, cache_config):
        cache = FDPCache(cache_config)
        fdp = _make_fdp('https://example.org/fdp')
        ds = _make_dataset('https://example.org/ds/1', fdp.uri)
        with _install_mock_client(cache, fdp, [ds]):
            cache.fetch_and_cache_fdp(fdp.uri)

        info = cache.get_cache_info()
        assert info['fdp_count'] == 1
        assert info['dataset_count'] == 1
        assert info['last_updated'] is not None

    def test_get_dataset_by_uri(self, cache_config):
        cache = FDPCache(cache_config)
        fdp = _make_fdp('https://example.org/fdp')
        ds = _make_dataset('https://example.org/ds/1', fdp.uri)
        with _install_mock_client(cache, fdp, [ds]):
            cache.fetch_and_cache_fdp(fdp.uri)

        assert cache.get_dataset_by_uri(ds.uri)['uri'] == ds.uri
        assert cache.get_dataset_by_uri('https://unknown') is None


class TestPopulateDefaults:
    def test_populate_defaults_fetches_all_configured(self, cache_config):
        cache_config = dict(cache_config, DEFAULT_FDPS=[
            'https://a.org/fdp', 'https://b.org/fdp'
        ])
        cache = FDPCache(cache_config)
        fetched = []

        def _fake_fetch(uri):
            fetched.append(uri)
            from app.services.cache import FDPCacheEntry
            from datetime import datetime
            entry = FDPCacheEntry(
                fdp_dict={'uri': uri, 'title': uri},
                datasets=[],
                last_updated=datetime.utcnow(),
            )
            with cache._lock:
                cache._entries[uri] = entry
            return entry

        with patch.object(cache, 'fetch_and_cache_fdp', side_effect=_fake_fetch):
            cache.populate_defaults()

        assert sorted(fetched) == ['https://a.org/fdp', 'https://b.org/fdp']
        assert cache.get_cache_info()['fdp_count'] == 2

    def test_populate_defaults_empty_is_noop(self, cache_config):
        cache = FDPCache(cache_config)
        cache.populate_defaults()  # should not raise
        assert cache.get_cache_info()['fdp_count'] == 0


class TestBackgroundRefresh:
    def test_start_and_stop_cleanly(self, cache_config):
        cache = FDPCache(cache_config)
        cache.start_background_refresh()
        assert cache._refresh_thread is not None
        assert cache._refresh_thread.is_alive()
        cache.stop_background_refresh(timeout=2.0)
        assert cache._refresh_thread is None

    def test_background_refresh_invokes_fetch(self, cache_config):
        cache = FDPCache(cache_config)
        fdp = _make_fdp('https://example.org/fdp')
        with _install_mock_client(cache, fdp, []):
            cache.fetch_and_cache_fdp(fdp.uri)

        calls = []
        original = cache.fetch_and_cache_fdp

        def counting(uri):
            calls.append(uri)
            return original(uri)

        # We still need the underlying fetch to succeed during the bg loop.
        client_mock = MagicMock()
        client_mock.fetch_fdp.return_value = fdp
        client_mock.fetch_catalog_with_datasets.return_value = []

        with patch.object(cache, '_make_client', return_value=client_mock), \
             patch.object(cache, 'fetch_and_cache_fdp', side_effect=counting):
            cache.start_background_refresh()
            time.sleep(0.35)  # interval = 0.1s → at least one iteration
            cache.stop_background_refresh(timeout=2.0)

        assert any(c == fdp.uri for c in calls), 'background loop should refresh cached FDP'
