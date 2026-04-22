"""Dashboard service — predefined aggregate queries, execution, and caching."""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from flask import current_app

from app.config import Config
from app.models.auth import EndpointCredentials
from app.services.fdp_client import FDPClient, FDPError
from app.services.dataset_service import DatasetService
from app.services.sparql_client import SPARQLClient, SPARQLError

logger = logging.getLogger(__name__)

_lock = threading.Lock()

_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'dashboard')
_CONFIG_FILE = os.path.join(_DATA_DIR, 'config.json')
_STATUS_FILE = os.path.join(_DATA_DIR, '_status.json')


# ---------------------------------------------------------------------------
# Predefined aggregate queries
# ---------------------------------------------------------------------------
# Each entry defines a query that runs against every configured endpoint.
# The ``transform`` callable receives raw SPARQL bindings and returns a
# display-ready dict.  Add new queries here; no template changes needed for
# simple stat-card or table display types.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Aggregate vocabulary
# ---------------------------------------------------------------------------
# The dashboard reads pre-computed aggregate triples from a dedicated
# dashboard repository on each AllegroGraph instance.  Data stewards
# populate that repository by running SPARQL INSERT queries (templates
# provided in app/data/dashboard/insert_queries.sparql).
#
# Namespace: <urn:hds:dashboard:>
#   :Statistic           — class for every aggregate row
#   :metric              — identifier string  (e.g. "total_records")
#   :dimension           — grouping key       (e.g. "Male", "Ethiopia")
#   :value               — numeric value      (xsd:integer)
#   :label               — human-readable label
#   :updatedAt           — xsd:dateTime of last computation
# ---------------------------------------------------------------------------

# Fetch all statistics from the dashboard repository using the HDS vocabulary.
_FETCH_ALL_STATS = """\
PREFIX hds: <urn:hds:dashboard:>

SELECT ?metric ?dimension ?value ?label
WHERE {
    ?stat a hds:Statistic ;
          hds:metric ?metric ;
          hds:value  ?value .
    OPTIONAL { ?stat hds:dimension ?dimension }
    OPTIONAL { ?stat hds:label     ?label }
}
ORDER BY ?metric ?dimension
"""


def _transform_stats(bindings: List[Dict]) -> Dict[str, Any]:
    """Transform HDS stat bindings into a structured dict grouped by metric.

    Handles three types of metrics:
    - Scalar: single value (e.g. total_participants) -> stat card
    - Dimensional: grouped values (e.g. country breakdown) -> bar chart
    - Range: earliest/latest dimensions (e.g. time_range) -> displayed as range
    """
    metrics: Dict[str, Any] = {}

    for row in bindings:
        metric = row.get('metric', {}).get('value', '')
        if not metric:
            continue

        value_str = row.get('value', {}).get('value', '0')
        try:
            value = int(value_str)
        except (ValueError, TypeError):
            try:
                value = float(value_str)
            except (ValueError, TypeError):
                value = 0

        dimension = row.get('dimension', {}).get('value', '') if 'dimension' in row else ''
        label = row.get('label', {}).get('value', '') if 'label' in row else ''

        if dimension:
            if metric not in metrics:
                metrics[metric] = {'dimensions': {}, 'label': label or metric}

            if metric == 'time_range':
                # For time ranges, take min of 'earliest' and max of 'latest'
                existing = metrics[metric]['dimensions'].get(dimension)
                if existing is None:
                    metrics[metric]['dimensions'][dimension] = value
                elif dimension == 'earliest':
                    metrics[metric]['dimensions'][dimension] = min(existing, value)
                elif dimension == 'latest':
                    metrics[metric]['dimensions'][dimension] = max(existing, value)
            else:
                metrics[metric]['dimensions'][dimension] = (
                    metrics[metric]['dimensions'].get(dimension, 0) + value
                )
        else:
            # Scalar metric
            if metric not in metrics:
                metrics[metric] = {'value': 0, 'label': label or metric}
            metrics[metric]['value'] = metrics[metric].get('value', 0) + value

    return metrics


def get_fdp_themes() -> List[Dict[str, Any]]:
    """Extract live themes from the configured FDPs."""
    try:
        fdp_client = FDPClient(
            timeout=Config.FDP_TIMEOUT,
            verify_ssl=Config.FDP_VERIFY_SSL,
        )
        dataset_service = DatasetService(fdp_client)
        datasets = dataset_service.get_all_datasets(list(current_app.config.get('DEFAULT_FDPS', [])))
        themes = dataset_service.get_available_themes(datasets)
        return [{'label': t.label, 'uri': t.uri, 'count': t.count} for t in themes]
    except Exception as e:
        logger.error(f'Failed to extract FDP themes: {e}')
        return []


DASHBOARD_QUERIES = [
    {
        'id': 'all_stats',
        'name': 'Aggregate Statistics',
        'description': 'Pre-computed aggregate statistics from all facilities',
        'sparql': _FETCH_ALL_STATS,
        'transform': _transform_stats,
        'display_type': 'auto',
    },
]


# ---------------------------------------------------------------------------
# Config management (endpoint list)
# ---------------------------------------------------------------------------

def get_config() -> Dict[str, Any]:
    """Read dashboard config, falling back to defaults."""
    os.makedirs(_DATA_DIR, exist_ok=True)
    if os.path.exists(_CONFIG_FILE):
        try:
            with open(_CONFIG_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning('Could not read dashboard config, using defaults')
    return {'endpoints': [], 'refresh_interval_override': None}


def save_config(config: Dict[str, Any]) -> None:
    """Save dashboard config atomically."""
    os.makedirs(_DATA_DIR, exist_ok=True)
    tmp = _CONFIG_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(config, f, indent=2)
    os.replace(tmp, _CONFIG_FILE)


def get_endpoints() -> List[Dict[str, str]]:
    """Return the list of dashboard SPARQL endpoints.

    Uses configured endpoints if available, otherwise falls back to
    auto-discovered endpoints from the last discovery run.
    """
    config = get_config()
    endpoints = config.get('endpoints', [])
    return [ep for ep in endpoints if ep.get('enabled', True)]


def _derive_dashboard_url(raw_endpoint_url: str) -> Optional[str]:
    """Derive the dashboard repository URL from a raw data endpoint URL.

    For AllegroGraph Cloud URLs like:
        https://ag1xyz.allegrograph.cloud/repositories/KS01/sparql
    Returns:
        https://ag1xyz.allegrograph.cloud/repositories/Dashboard/sparql

    The dashboard repo name comes from Config.DASHBOARD_REPO_NAME.
    """
    import re
    repo_name = Config.DASHBOARD_REPO_NAME
    # Match AllegroGraph URL pattern: .../repositories/<name>... and extract server base
    match = re.match(r'(https?://[^/]+/repositories/)[^/]+(/sparql)?.*', raw_endpoint_url)
    if match:
        # Always use /sparql suffix for the dashboard endpoint
        return f'{match.group(1)}{repo_name}/sparql'
    return None


def discover_endpoints() -> List[Dict[str, str]]:
    """Discover dashboard SPARQL endpoints by crawling all default FDPs.

    Reuses the same FDP -> catalog -> dataset -> distribution pipeline that
    the dataset browse page uses.  Finds all AllegroGraph SPARQL endpoints,
    then derives the dashboard repository URL for each unique AG server.

    Each AG server is expected to have a dashboard repository (configured
    via DASHBOARD_REPO_NAME) that contains pre-computed aggregate statistics.

    Returns:
        List of endpoint dicts with 'url' and 'label' keys.
    """
    logger.info('Starting SPARQL endpoint discovery from FDPs')

    fdp_client = FDPClient(
        timeout=Config.FDP_TIMEOUT,
        verify_ssl=Config.FDP_VERIFY_SSL,
    )
    dataset_service = DatasetService(fdp_client)

    fdp_uris = list(current_app.config.get('DEFAULT_FDPS', []))

    # Also include any FDP URIs from existing config (manual additions)
    config = get_config()
    for ep in config.get('extra_fdps', []):
        uri = ep if isinstance(ep, str) else ep.get('url', '')
        if uri and uri not in fdp_uris:
            fdp_uris.append(uri)

    try:
        datasets = dataset_service.get_all_datasets(fdp_uris)
    except Exception as e:
        logger.error(f'Failed to fetch datasets for endpoint discovery: {e}')
        return get_endpoints()  # fall back to whatever was previously saved

    # Extract unique AG servers from discovered SPARQL endpoints,
    # then derive the dashboard repo URL for each.
    seen_servers = set()
    endpoints = []

    for ds in datasets:
        for dist in ds.distributions:
            if not dist.is_sparql_endpoint:
                continue
            url = dist.endpoint_url or dist.access_url
            if not url:
                continue

            dashboard_url = _derive_dashboard_url(url)
            if not dashboard_url or dashboard_url in seen_servers:
                continue
            seen_servers.add(dashboard_url)

            # Use the AG server hostname as the label
            import re
            server_match = re.match(r'https?://([^/]+)', url)
            server_label = server_match.group(1) if server_match else url

            endpoints.append({
                'url': dashboard_url,
                'label': server_label,
                'enabled': True,
                'discovered': True,
                'derived_from': url,
            })

    logger.info(f'Discovered {len(endpoints)} SPARQL endpoint(s) from {len(fdp_uris)} FDP(s)')

    # Merge with any manually added endpoints (preserve manual entries)
    existing = config.get('endpoints', [])
    manual_endpoints = [ep for ep in existing if not ep.get('discovered', False)]
    manual_urls = {ep['url'] for ep in manual_endpoints}

    merged = list(manual_endpoints)
    for ep in endpoints:
        if ep['url'] not in manual_urls:
            merged.append(ep)

    # Save discovered endpoints to config
    config['endpoints'] = merged
    save_config(config)

    return [ep for ep in merged if ep.get('enabled', True)]


# ---------------------------------------------------------------------------
# Refresh logic
# ---------------------------------------------------------------------------

def _write_status(status: Dict[str, Any]) -> None:
    """Write refresh status atomically."""
    os.makedirs(_DATA_DIR, exist_ok=True)
    tmp = _STATUS_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(status, f, indent=2)
    os.replace(tmp, _STATUS_FILE)


def get_refresh_status() -> Dict[str, Any]:
    """Read current refresh status."""
    if os.path.exists(_STATUS_FILE):
        try:
            with open(_STATUS_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {
        'last_refresh': None,
        'refresh_in_progress': False,
        'errors': [],
    }


def refresh_all() -> bool:
    """Execute all predefined queries against all endpoints and cache results.

    Returns True if the refresh ran, False if skipped (already in progress).
    """
    if not _lock.acquire(blocking=False):
        logger.info('Dashboard refresh already in progress, skipping')
        return False

    try:
        logger.info('Starting dashboard data refresh')
        status = get_refresh_status()
        status['refresh_in_progress'] = True
        status['errors'] = []
        _write_status(status)

        # Step 1: Discover SPARQL endpoints from FDPs
        endpoints = discover_endpoints()
        if not endpoints:
            logger.warning('No SPARQL endpoints discovered, skipping query execution')
            status['refresh_in_progress'] = False
            status['last_refresh'] = datetime.now(timezone.utc).isoformat()
            status['discovered_endpoints'] = 0
            _write_status(status)
            return True

        status['discovered_endpoints'] = len(endpoints)
        _write_status(status)

        credentials = EndpointCredentials(
            fdp_uri='dashboard',
            sparql_endpoint='',
            username=Config.DASHBOARD_SPARQL_USERNAME,
            password=Config.DASHBOARD_SPARQL_PASSWORD,
        )

        client = SPARQLClient(timeout=Config.DASHBOARD_SPARQL_TIMEOUT)

        for query_def in DASHBOARD_QUERIES:
            query_id = query_def['id']
            query_result = {
                'query_id': query_id,
                'query_name': query_def['name'],
                'description': query_def['description'],
                'display_type': query_def['display_type'],
                'refreshed_at': datetime.now(timezone.utc).isoformat(),
                'endpoints': {},
                'aggregated': {},
            }

            all_bindings = []

            for ep in endpoints:
                ep_url = ep['url']
                ep_label = ep.get('label', ep_url)
                start_time = time.time()

                try:
                    result = client.execute_query(ep_url, query_def['sparql'], credentials)
                    exec_ms = int((time.time() - start_time) * 1000)

                    ep_transformed = query_def['transform'](result['bindings'])

                    query_result['endpoints'][ep_url] = {
                        'label': ep_label,
                        'success': True,
                        'execution_time_ms': exec_ms,
                        'data': ep_transformed,
                        'binding_count': len(result['bindings']),
                    }
                    all_bindings.extend(result['bindings'])

                except SPARQLError as e:
                    exec_ms = int((time.time() - start_time) * 1000)
                    logger.warning(f'Dashboard query {query_id} failed for {ep_url}: {e}')
                    query_result['endpoints'][ep_url] = {
                        'label': ep_label,
                        'success': False,
                        'execution_time_ms': exec_ms,
                        'error': str(e),
                    }
                    status['errors'].append(f'{query_id} @ {ep_label}: {e}')

                except Exception as e:
                    logger.error(f'Unexpected error in dashboard query {query_id} for {ep_url}: {e}')
                    query_result['endpoints'][ep_url] = {
                        'label': ep_label,
                        'success': False,
                        'error': str(e),
                    }
                    status['errors'].append(f'{query_id} @ {ep_label}: {e}')

            # Aggregate across all endpoints
            query_result['aggregated'] = query_def['transform'](all_bindings)
            successful = sum(1 for ep in query_result['endpoints'].values() if ep.get('success'))
            query_result['successful_endpoints'] = successful
            query_result['total_endpoints'] = len(endpoints)

            # Write result atomically
            result_file = os.path.join(_DATA_DIR, f'{query_id}.json')
            tmp = result_file + '.tmp'
            with open(tmp, 'w') as f:
                json.dump(query_result, f, indent=2)
            os.replace(tmp, result_file)

            logger.info(f'Dashboard query {query_id}: {successful}/{len(endpoints)} endpoints succeeded')

        # Step 3: Fetch and cache FDP themes
        themes = get_fdp_themes()
        themes_file = os.path.join(_DATA_DIR, 'themes.json')
        tmp = themes_file + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'themes': themes, 'refreshed_at': datetime.now(timezone.utc).isoformat()}, f, indent=2)
        os.replace(tmp, themes_file)
        logger.info(f'Cached {len(themes)} FDP themes')

        status['refresh_in_progress'] = False
        status['last_refresh'] = datetime.now(timezone.utc).isoformat()
        _write_status(status)
        logger.info('Dashboard data refresh complete')
        return True

    except Exception as e:
        logger.error(f'Dashboard refresh failed: {e}')
        status = get_refresh_status()
        status['refresh_in_progress'] = False
        status['errors'].append(f'Refresh failed: {e}')
        _write_status(status)
        return False

    finally:
        _lock.release()


# ---------------------------------------------------------------------------
# Data reading (for the public dashboard page)
# ---------------------------------------------------------------------------

def get_dashboard_data() -> Dict[str, Any]:
    """Read all cached query results for display.

    Returns a dict keyed by query_id with the cached result data,
    plus a 'status' key with refresh metadata.
    """
    data = {'queries': {}, 'status': get_refresh_status(), 'themes': []}

    for query_def in DASHBOARD_QUERIES:
        query_id = query_def['id']
        result_file = os.path.join(_DATA_DIR, f'{query_id}.json')
        if os.path.exists(result_file):
            try:
                with open(result_file, 'r') as f:
                    data['queries'][query_id] = json.load(f)
            except (json.JSONDecodeError, OSError):
                logger.warning(f'Could not read cached data for {query_id}')

    # Load cached themes
    themes_file = os.path.join(_DATA_DIR, 'themes.json')
    if os.path.exists(themes_file):
        try:
            with open(themes_file, 'r') as f:
                data['themes'] = json.load(f).get('themes', [])
        except (json.JSONDecodeError, OSError):
            pass

    return data
