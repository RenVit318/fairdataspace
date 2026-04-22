"""Flask application factory for the Data Visiting PoC."""

import importlib.util
import logging
import os
from typing import Optional, Dict, Any

from flask import Blueprint, Flask
from jinja2 import ChoiceLoader, FileSystemLoader

from app.config import Config


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DATASPACES_DIR = os.path.join(_REPO_ROOT, 'dataspaces')


def _load_dataspace(app: Flask) -> None:
    """Load the selected dataspace's config, static files, and template overrides."""
    name = os.environ.get('DATASPACE', 'humanitarian')
    ds_dir = os.path.join(_DATASPACES_DIR, name)

    if not os.path.isdir(ds_dir):
        raise RuntimeError(
            f"Dataspace '{name}' not found at {ds_dir}. "
            f"Available: {sorted(os.listdir(_DATASPACES_DIR))}"
        )

    config_path = os.path.join(ds_dir, 'config.py')
    spec = importlib.util.spec_from_file_location(f'dataspace_{name}', config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for key in dir(module):
        if key.isupper():
            app.config[key] = getattr(module, key)

    app.config['DATASPACE'] = name
    app.config['DATASPACE_DIR'] = ds_dir

    ds_static = os.path.join(ds_dir, 'static')
    if os.path.isdir(ds_static):
        ds_bp = Blueprint(
            'dataspace_static',
            __name__,
            static_folder=ds_static,
            static_url_path='/dataspace-static',
        )
        app.register_blueprint(ds_bp)

    ds_templates = os.path.join(ds_dir, 'templates')
    if os.path.isdir(ds_templates):
        app.jinja_loader = ChoiceLoader([
            FileSystemLoader(ds_templates),
            app.jinja_loader,
        ])

    @app.context_processor
    def _inject_site_config():
        return {
            'site': {
                'name': app.config.get('SITE_NAME', ''),
                'tagline': app.config.get('SITE_TAGLINE', ''),
                'contact_email': app.config.get('CONTACT_EMAIL', ''),
                'brand_logos': app.config.get('BRAND_LOGOS', []),
                'dataspace': name,
            }
        }


def _seed_default_fdps(session) -> None:
    """Populate a new session with the configured default FDP endpoints."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from flask import current_app
    from app.services import FDPClient
    from app.utils import get_uri_hash

    default_uris = current_app.config.get('DEFAULT_FDPS', [])
    if not default_uris:
        return

    client = FDPClient(
        timeout=current_app.config.get('FDP_TIMEOUT', 30),
        verify_ssl=current_app.config.get('FDP_VERIFY_SSL', True),
    )

    def _fetch(uri):
        try:
            return client.fetch_fdp(uri)
        except Exception as e:
            logging.getLogger(__name__).warning(f"Could not fetch default FDP {uri}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=len(default_uris)) as pool:
        futures = {pool.submit(_fetch, uri): uri for uri in default_uris}
        for future in as_completed(futures):
            fdp = future.result()
            if fdp:
                session['fdps'][get_uri_hash(fdp.uri)] = fdp.to_dict()

    session.modified = True


def create_app(config_override: Optional[Dict[str, Any]] = None) -> Flask:
    """
    Create and configure the Flask application.

    Args:
        config_override: Optional dictionary of configuration overrides.

    Returns:
        Configured Flask application instance.
    """
    app = Flask(__name__)

    # Load configuration
    app.config.from_object(Config)

    # Load the selected dataspace (branding, default FDPs, static, templates)
    _load_dataspace(app)

    # Apply any overrides (tests pass overrides last so they win)
    if config_override:
        app.config.update(config_override)

    # Initialize server-side sessions (filesystem-backed)
    from flask_session import Session
    Session(app)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, app.config.get('LOG_LEVEL', 'INFO')),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Register blueprints
    from app.routes.main import main_bp
    from app.routes.fdp import fdp_bp
    from app.routes.datasets import datasets_bp
    from app.routes.request import request_bp
    from app.routes.auth import auth_bp
    from app.routes.sparql import sparql_bp
    from app.routes.admin import admin_bp
    from app.routes.dashboard import dashboard_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(fdp_bp)
    app.register_blueprint(datasets_bp)
    app.register_blueprint(request_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(sparql_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(dashboard_bp)

    # Initialize session defaults
    @app.before_request
    def init_session():
        from flask import session
        if 'fdps' not in session:
            session['fdps'] = {}
            # Seed default FDPs for new sessions
            _seed_default_fdps(session)
        if 'basket' not in session:
            session['basket'] = []
        if 'datasets_cache' not in session:
            session['datasets_cache'] = []
        if 'endpoint_credentials' not in session:
            session['endpoint_credentials'] = {}
        if 'discovered_endpoints' not in session:
            session['discovered_endpoints'] = {}

    # Ensure dashboard data directory exists
    os.makedirs(os.path.join(app.root_path, 'data', 'dashboard'), exist_ok=True)

    # Initialize dashboard scheduler (skip in testing)
    if not app.config.get('TESTING'):
        from app.services.dashboard_scheduler import init_scheduler
        init_scheduler(app)

    # Security headers
    @app.after_request
    def set_security_headers(response):
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        return response

    return app
