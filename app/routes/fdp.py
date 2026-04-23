"""FDP management routes."""

from typing import Optional

from flask import Blueprint, current_app, render_template, request, session, flash, redirect, url_for

from app.config import Config
from app.routes.admin import admin_required
from app.services import FDPClient, FDPConnectionError, FDPParseError, FDPTimeoutError
from app.utils import get_uri_hash

fdp_bp = Blueprint('fdp', __name__, url_prefix='/fdp')


def _fdps_for_display() -> dict:
    """Build the {uri_hash: fdp_dict} map the template expects, sourced from the cache."""
    cache = current_app.fdp_cache
    fdp_uris = session.get('fdp_uris', [])
    out: dict = {}
    for uri in fdp_uris:
        fdp_dict = cache.get_fdp(uri)
        if fdp_dict is None:
            # Known URI but not in cache yet (e.g. first load before populate finishes
            # or a prior refresh error with no previous data). Show a placeholder.
            fdp_dict = {
                'uri': uri,
                'title': uri,
                'description': None,
                'publisher': None,
                'is_index': False,
                'catalogs': [],
                'linked_fdps': [],
                'status': 'pending',
                'error_message': None,
                'last_fetched': None,
            }
        else:
            entry = cache.get_entry(uri)
            if entry and entry.error:
                # Preserve whatever dict the cache returned, but surface the error.
                fdp_dict = dict(fdp_dict)
                fdp_dict['error_message'] = entry.error
                fdp_dict['status'] = 'error'
        out[get_uri_hash(uri)] = fdp_dict
    return out


@fdp_bp.route('/')
def list_fdps():
    """Public read-only list of configured FDPs."""
    fdps = _fdps_for_display()
    is_admin = session.get('is_admin', False)
    cache_info = current_app.fdp_cache.get_cache_info()
    return render_template(
        'fdp/list.html', fdps=fdps, is_admin=is_admin, cache_info=cache_info
    )


@fdp_bp.route('/add', methods=['GET', 'POST'])
@admin_required
def add():
    """Add a new FDP endpoint."""
    if request.method == 'POST':
        url = request.form.get('url', '').strip()
        is_index = request.form.get('is_index') == 'on'

        if not url:
            flash('Please enter a valid URL.', 'error')
            return render_template('fdp/add.html')

        # Validate URL format
        if not url.startswith(('http://', 'https://')):
            flash('URL must start with http:// or https://', 'error')
            return render_template('fdp/add.html', url=url, is_index=is_index)

        fdp_uris = session.setdefault('fdp_uris', [])
        cache = current_app.fdp_cache

        # Check if already added
        if url in fdp_uris:
            flash('This FDP is already configured.', 'warning')
            return redirect(url_for('fdp.list_fdps'))

        client = FDPClient(timeout=Config.FDP_TIMEOUT, verify_ssl=Config.FDP_VERIFY_SSL)

        try:
            if is_index:
                # Fetch index FDP and discover linked FDPs
                fdps = client.fetch_all_from_index(url)
                added_count = 0
                for fdp in fdps:
                    if fdp.uri in fdp_uris:
                        continue
                    cache.fetch_and_cache_fdp(fdp.uri)
                    fdp_uris.append(fdp.uri)
                    added_count += 1
                session['fdp_uris'] = fdp_uris
                session.modified = True

                if added_count > 0:
                    flash(f'Successfully added {added_count} FDP(s).', 'success')
                else:
                    flash('No new FDPs to add.', 'info')
            else:
                # Fetch single FDP (via cache so datasets are populated too)
                entry = cache.fetch_and_cache_fdp(url)
                if entry is None:
                    flash(
                        'Could not fetch this FAIR Data Point. Please check the URL.',
                        'error',
                    )
                    return render_template('fdp/add.html', url=url, is_index=is_index)
                fdp_uris.append(url)
                session['fdp_uris'] = fdp_uris
                session.modified = True
                flash(
                    f'Successfully added FDP: {entry.fdp_dict.get("title") or url}',
                    'success',
                )

            return redirect(url_for('fdp.list_fdps'))

        except FDPConnectionError:
            flash('Could not connect to the FAIR Data Point. Please check the URL.', 'error')
            return render_template('fdp/add.html', url=url, is_index=is_index)
        except FDPParseError:
            flash('Could not parse the FDP metadata. The endpoint may not be a valid FDP.', 'error')
            return render_template('fdp/add.html', url=url, is_index=is_index)
        except FDPTimeoutError:
            flash('Request timed out. Please try again.', 'error')
            return render_template('fdp/add.html', url=url, is_index=is_index)

    return render_template('fdp/add.html')


def _find_uri_by_hash(uri_hash: str) -> Optional[str]:
    for uri in session.get('fdp_uris', []):
        if get_uri_hash(uri) == uri_hash:
            return uri
    return None


@fdp_bp.route('/<uri_hash>/refresh', methods=['POST'])
@admin_required
def refresh(uri_hash: str):
    """Refresh FDP metadata + datasets via the process-wide cache."""
    uri = _find_uri_by_hash(uri_hash)
    if uri is None:
        flash('FDP not found.', 'error')
        return redirect(url_for('fdp.list_fdps'))

    entry = current_app.fdp_cache.fetch_and_cache_fdp(uri)
    if entry is None or entry.error:
        message = entry.error if (entry and entry.error) else 'Unknown error'
        flash(f'Could not refresh FDP: {message}', 'error')
    else:
        flash(f'Successfully refreshed FDP: {entry.fdp_dict.get("title") or uri}', 'success')

    return redirect(url_for('fdp.list_fdps'))


@fdp_bp.route('/<uri_hash>/remove', methods=['POST'])
@admin_required
def remove(uri_hash: str):
    """Remove an FDP from the admin's session list. Cache entry stays (other users may reference it)."""
    uri = _find_uri_by_hash(uri_hash)
    if uri is None:
        flash('FDP not found.', 'error')
        return redirect(url_for('fdp.list_fdps'))

    fdp_dict = current_app.fdp_cache.get_fdp(uri) or {}
    title = fdp_dict.get('title') or uri

    session['fdp_uris'] = [u for u in session.get('fdp_uris', []) if u != uri]
    session.modified = True

    flash(f'Removed FDP: {title}', 'success')
    return redirect(url_for('fdp.list_fdps'))
