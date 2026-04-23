"""Flask application factory for the fairdataspace."""

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

    # Initialize process-wide FDP cache
    from app.services import FDPCache
    app.fdp_cache = FDPCache(app.config)

    # Populate cache and start background refresh (skip in tests; guard Werkzeug reloader)
    if not app.config.get('TESTING'):
        is_reloader_parent = (
            app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true'
        )
        if not is_reloader_parent:
            app.fdp_cache.populate_defaults()
            app.fdp_cache.start_background_refresh()

    # Initialize session defaults
    @app.before_request
    def init_session():
        from flask import session, current_app

        # Migrate legacy session['fdps'] (dict of full FDP data) → session['fdp_uris'] (list).
        if 'fdp_uris' not in session:
            if 'fdps' in session and isinstance(session['fdps'], dict):
                session['fdp_uris'] = [
                    v['uri'] for v in session['fdps'].values()
                    if isinstance(v, dict) and v.get('uri')
                ]
            else:
                default_uris = current_app.config.get('DEFAULT_FDPS', []) or []
                session['fdp_uris'] = list(default_uris)

        # Drop legacy session keys that are now replaced by the process-wide cache.
        session.pop('fdps', None)
        session.pop('datasets_cache', None)

        if 'basket' not in session:
            session['basket'] = []
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
