"""Flask application factory for the Data Visiting PoC."""

import logging
from typing import Optional, Dict, Any

from flask import Flask

from app.config import Config


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

    # Apply any overrides
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

    app.register_blueprint(main_bp)
    app.register_blueprint(fdp_bp)
    app.register_blueprint(datasets_bp)
    app.register_blueprint(request_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(sparql_bp)

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

    # Security headers
    @app.after_request
    def set_security_headers(response):
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        return response

    return app
