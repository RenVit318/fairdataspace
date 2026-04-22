"""Admin data management - credentials and page content stored on disk."""

import json
import os
import threading

from flask import current_app, has_app_context
from werkzeug.security import generate_password_hash, check_password_hash

_lock = threading.Lock()

# Persistent JSON file that holds admin credentials and editable page content.
_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
_ADMIN_FILE = os.path.join(_DATA_DIR, 'admin.json')


def _load_default_pages():
    """Load seed page content from the active dataspace's pages/ directory."""
    if not has_app_context():
        return {}
    ds_dir = current_app.config.get('DATASPACE_DIR')
    if not ds_dir:
        return {}
    pages_dir = os.path.join(ds_dir, 'pages')
    if not os.path.isdir(pages_dir):
        return {}
    defaults = {}
    for entry in os.listdir(pages_dir):
        if not entry.endswith('.json'):
            continue
        key = entry[:-5]
        with open(os.path.join(pages_dir, entry), 'r') as f:
            defaults[key] = json.load(f)
    return defaults


def _read_data():
    """Read the admin JSON file, returning a dict."""
    if not os.path.exists(_ADMIN_FILE):
        return {}
    with open(_ADMIN_FILE, 'r') as f:
        return json.load(f)


def _write_data(data):
    """Atomically write the admin JSON file."""
    os.makedirs(_DATA_DIR, exist_ok=True)
    tmp = _ADMIN_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, _ADMIN_FILE)


def _ensure_admin():
    """Create the default admin account if none exists yet."""
    with _lock:
        data = _read_data()
        if 'admin' not in data:
            data['admin'] = {
                'username': 'admin',
                'password_hash': generate_password_hash('admin'),
            }
            _write_data(data)
        if 'pages' not in data:
            data['pages'] = _load_default_pages()
            _write_data(data)
    return data


def verify_admin(username, password):
    """Return True if username/password match the stored admin credentials."""
    data = _ensure_admin()
    admin = data.get('admin', {})
    if username != admin.get('username'):
        return False
    return check_password_hash(admin['password_hash'], password)


def change_admin_password(new_password):
    """Update the admin password (stores hash, never plaintext)."""
    with _lock:
        data = _ensure_admin()
        data['admin']['password_hash'] = generate_password_hash(new_password)
        _write_data(data)


def get_page_content(page_key):
    """Return the editable content dict for a given page, or defaults."""
    data = _ensure_admin()
    defaults = _load_default_pages()
    pages = data.get('pages', defaults)
    return pages.get(page_key, defaults.get(page_key, {}))


def save_page_content(page_key, content):
    """Save edited page content."""
    with _lock:
        data = _ensure_admin()
        if 'pages' not in data:
            data['pages'] = dict(_load_default_pages())
        data['pages'][page_key] = content
        _write_data(data)


def get_all_page_keys():
    """Return a list of editable page keys."""
    return list(_load_default_pages().keys())


def get_default_fields(page_key):
    """Return the field names for a given page (used to build the edit form)."""
    return _load_default_pages().get(page_key, {})
