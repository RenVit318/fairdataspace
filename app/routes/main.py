"""Main routes for the Data Visiting PoC application."""

from flask import Blueprint, render_template, session

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """Render the landing page."""
    fdp_count = len(session.get('fdps', {}))
    basket_count = len(session.get('basket', []))

    return render_template(
        'index.html',
        fdp_count=fdp_count,
        basket_count=basket_count,
    )


@main_bp.route('/about')
def about():
    """Render the about page."""
    return render_template('about.html')
