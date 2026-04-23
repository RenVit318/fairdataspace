"""Dataset browsing routes."""

import logging

from flask import Blueprint, current_app, render_template, request, session, flash, redirect, url_for

from app.config import Config
from app.models import Dataset, ContactPoint, Distribution
from app.services import FDPClient, DatasetService
from app.utils import get_uri_hash

logger = logging.getLogger(__name__)

datasets_bp = Blueprint('datasets', __name__, url_prefix='/datasets')

DATASETS_PER_PAGE = 10


def dataset_from_dict(data: dict) -> Dataset:
    """Reconstruct Dataset from dictionary."""
    contact_data = data.get('contact_point')
    contact_point = None
    if contact_data:
        contact_point = ContactPoint(
            name=contact_data.get('name'),
            email=contact_data.get('email'),
            url=contact_data.get('url'),
        )

    raw_dists = data.get('distributions', [])
    distributions = []
    for d in raw_dists:
        if isinstance(d, dict):
            distributions.append(Distribution.from_dict(d))
        elif isinstance(d, str):
            distributions.append(Distribution(uri=d))

    return Dataset(
        uri=data['uri'],
        title=data['title'],
        catalog_uri=data['catalog_uri'],
        catalog_title=data.get('catalog_title'),
        catalog_homepage=data.get('catalog_homepage'),
        fdp_uri=data['fdp_uri'],
        fdp_title=data['fdp_title'],
        description=data.get('description'),
        publisher=data.get('publisher'),
        creator=data.get('creator'),
        issued=None,
        modified=None,
        themes=data.get('themes', []),
        theme_labels=data.get('theme_labels', []),
        keywords=data.get('keywords', []),
        contact_point=contact_point,
        landing_page=data.get('landing_page'),
        distributions=distributions,
    )


def _get_cached_datasets() -> list:
    """Return the dataset dicts in the cache visible to this session."""
    cache = current_app.fdp_cache
    fdp_uris = session.get('fdp_uris', [])
    return cache.get_datasets_for_fdps(fdp_uris)


@datasets_bp.route('/')
def browse():
    """Browse all datasets with filtering and pagination."""
    # Get filter parameters
    query = request.args.get('q', '').strip()
    theme_filter = request.args.get('theme', '').strip()
    app_filter = request.args.get('app', '').strip()
    sort_by = request.args.get('sort', 'title')
    page = request.args.get('page', 1, type=int)

    datasets_dicts = _get_cached_datasets()

    # Convert to Dataset objects for filtering
    datasets = [dataset_from_dict(d) for d in datasets_dicts]

    # Initialize service for filtering (no client needed for filtering)
    client = FDPClient(timeout=Config.FDP_TIMEOUT, verify_ssl=Config.FDP_VERIFY_SSL)
    service = DatasetService(client)

    # Get available themes and applications before filtering so the dropdowns
    # reflect the full universe, not the current subset.
    themes = service.get_available_themes(datasets)
    applications = service.get_available_applications(datasets)

    if query:
        datasets = service.search(datasets, query)
    if theme_filter:
        datasets = service.filter_by_theme(datasets, theme_filter)
    if app_filter:
        datasets = service.filter_by_application(datasets, app_filter)

    if sort_by == 'title':
        datasets.sort(key=lambda d: (d.title or '').lower())
    elif sort_by == 'modified':
        datasets.sort(key=lambda d: d.modified or '', reverse=True)
    elif sort_by == 'fdp':
        datasets.sort(key=lambda d: (d.fdp_title or '').lower())

    total_datasets = len(datasets)
    total_pages = (total_datasets + DATASETS_PER_PAGE - 1) // DATASETS_PER_PAGE
    page = max(1, min(page, total_pages)) if total_pages > 0 else 1

    start_idx = (page - 1) * DATASETS_PER_PAGE
    end_idx = start_idx + DATASETS_PER_PAGE
    paginated_datasets = datasets[start_idx:end_idx]

    basket = session.get('basket', [])
    basket_uris = {item['uri'] for item in basket}

    cache_info = current_app.fdp_cache.get_cache_info()

    return render_template(
        'datasets/browse.html',
        datasets=paginated_datasets,
        themes=themes,
        applications=applications,
        query=query,
        theme_filter=theme_filter,
        app_filter=app_filter,
        sort_by=sort_by,
        page=page,
        total_pages=total_pages,
        total_datasets=total_datasets,
        basket_uris=basket_uris,
        get_uri_hash=get_uri_hash,
        cache_info=cache_info,
    )


@datasets_bp.route('/refresh', methods=['POST'])
def refresh():
    """Force a cache refresh for every FDP known to this session."""
    fdp_uris = session.get('fdp_uris', [])

    if not fdp_uris:
        flash('No FDPs configured. Add an FDP first.', 'warning')
        return redirect(url_for('datasets.browse'))

    cache = current_app.fdp_cache
    errors = 0
    for uri in fdp_uris:
        entry = cache.fetch_and_cache_fdp(uri)
        if entry is None or entry.error:
            errors += 1

    datasets = cache.get_datasets_for_fdps(fdp_uris)
    if errors:
        flash(
            f'Refreshed with {errors} error(s); cache holds {len(datasets)} dataset(s).',
            'warning',
        )
    else:
        flash(f'Successfully refreshed {len(datasets)} dataset(s).', 'success')

    return redirect(url_for('datasets.browse'))


@datasets_bp.route('/<uri_hash>')
def detail(uri_hash: str):
    """Show dataset detail view, served entirely from cache."""
    datasets_dicts = _get_cached_datasets()

    dataset_dict = None
    for d in datasets_dicts:
        if get_uri_hash(d['uri']) == uri_hash:
            dataset_dict = d
            break

    if not dataset_dict:
        flash('Dataset not found.', 'error')
        return redirect(url_for('datasets.browse'))

    dataset = dataset_from_dict(dataset_dict)
    _store_discovered_endpoints(dataset)

    # Find siblings — other cached datasets in the same application.
    siblings_by_fdp: dict = {}
    if dataset.catalog_homepage:
        for d in datasets_dicts:
            if d.get('catalog_homepage') != dataset.catalog_homepage:
                continue
            if d['uri'] == dataset.uri:
                continue
            fdp_title = d.get('fdp_title') or d.get('fdp_uri') or ''
            siblings_by_fdp.setdefault(fdp_title, []).append({
                'uri': d['uri'],
                'uri_hash': get_uri_hash(d['uri']),
                'title': d['title'],
                'fdp_title': fdp_title,
            })

    basket = session.get('basket', [])
    in_basket = any(item['uri'] == dataset.uri for item in basket)
    basket_uris = {item['uri'] for item in basket}

    return render_template(
        'datasets/detail.html',
        dataset=dataset,
        uri_hash=uri_hash,
        in_basket=in_basket,
        siblings_by_fdp=siblings_by_fdp,
        basket_uris=basket_uris,
    )


@datasets_bp.route('/add-application-to-basket', methods=['POST'])
def add_application_to_basket():
    """Add every cached dataset with the given catalog_homepage to the basket."""
    homepage = (request.form.get('homepage') or '').strip()
    if not homepage:
        flash('No application selected.', 'error')
        return redirect(url_for('datasets.browse'))

    datasets_dicts = _get_cached_datasets()
    basket = session.get('basket', [])
    existing_uris = {item['uri'] for item in basket}

    added = 0
    for d in datasets_dicts:
        if d.get('catalog_homepage') != homepage:
            continue
        if d['uri'] in existing_uris:
            continue
        uri_hash = get_uri_hash(d['uri'])
        basket.append({
            'uri': d['uri'],
            'uri_hash': uri_hash,
            'title': d['title'],
            'fdp_title': d['fdp_title'],
            'catalog_uri': d.get('catalog_uri'),
            'catalog_title': d.get('catalog_title'),
            'catalog_homepage': d.get('catalog_homepage'),
            'contact_point': d.get('contact_point'),
        })
        existing_uris.add(d['uri'])
        added += 1

    session['basket'] = basket
    session.modified = True

    if added:
        flash(f'Added {added} dataset(s) from this application to your basket.', 'success')
    else:
        flash('All datasets for this application are already in your basket.', 'info')

    next_url = request.form.get('next') or request.referrer
    if not next_url or not next_url.startswith('/') or next_url.startswith('//'):
        next_url = url_for('datasets.browse')
    return redirect(next_url)


def _store_discovered_endpoints(dataset: Dataset) -> None:
    """Store discovered SPARQL endpoints in session for later credential config."""
    if 'discovered_endpoints' not in session:
        session['discovered_endpoints'] = {}

    for dist in dataset.distributions:
        if not dist.is_sparql_endpoint:
            continue
        url = dist.endpoint_url or dist.access_url
        if not url:
            continue
        endpoint_key = get_uri_hash(url)
        session['discovered_endpoints'][endpoint_key] = {
            'endpoint_url': url,
            'dataset_uri': dataset.uri,
            'dataset_title': dataset.title,
            'fdp_uri': dataset.fdp_uri,
            'fdp_title': dataset.fdp_title,
            'distribution_title': dist.title,
        }

    session.modified = True


@datasets_bp.route('/<uri_hash>/add-to-basket', methods=['POST'])
def add_to_basket(uri_hash: str):
    """Add a dataset to the request basket."""
    datasets_dicts = _get_cached_datasets()

    dataset_dict = None
    for d in datasets_dicts:
        if get_uri_hash(d['uri']) == uri_hash:
            dataset_dict = d
            break

    if not dataset_dict:
        flash('Dataset not found.', 'error')
        return redirect(url_for('datasets.browse'))

    basket = session.get('basket', [])
    if any(item['uri'] == dataset_dict['uri'] for item in basket):
        flash('Dataset is already in your basket.', 'info')
    else:
        # Full dataset (with distributions) comes from cache — no extra HTTP.
        _store_discovered_endpoints(dataset_from_dict(dataset_dict))

        basket.append({
            'uri': dataset_dict['uri'],
            'uri_hash': uri_hash,
            'title': dataset_dict['title'],
            'fdp_title': dataset_dict['fdp_title'],
            'catalog_uri': dataset_dict.get('catalog_uri'),
            'catalog_title': dataset_dict.get('catalog_title'),
            'catalog_homepage': dataset_dict.get('catalog_homepage'),
            'contact_point': dataset_dict.get('contact_point'),
        })
        session['basket'] = basket
        session.modified = True
        flash(f'Added "{dataset_dict["title"]}" to your basket.', 'success')

    next_url = request.form.get('next') or request.referrer
    if not next_url or not next_url.startswith('/') or next_url.startswith('//'):
        next_url = url_for('datasets.browse')
    return redirect(next_url)


@datasets_bp.route('/<uri_hash>/remove-from-basket', methods=['POST'])
def remove_from_basket(uri_hash: str):
    """Remove a dataset from the request basket."""
    basket = session.get('basket', [])

    new_basket = [item for item in basket if item.get('uri_hash') != uri_hash]

    if len(new_basket) == len(basket):
        flash('Dataset not found in basket.', 'error')
    else:
        session['basket'] = new_basket
        session.modified = True
        flash('Removed dataset from basket.', 'success')

    next_url = request.form.get('next') or request.referrer
    if not next_url or not next_url.startswith('/') or next_url.startswith('//'):
        next_url = url_for('datasets.browse')
    return redirect(next_url)
