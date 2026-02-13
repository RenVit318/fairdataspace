"""Integration tests for the Humanitarian Data Space application."""

import pytest
import responses
from flask import session

from app import create_app


@pytest.fixture
def app():
    """Create application for testing."""
    app = create_app({
        'TESTING': True,
        'SECRET_KEY': 'test-secret-key',
        'WTF_CSRF_ENABLED': False,
        'DEFAULT_FDPS': [],
    })
    yield app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


@pytest.fixture
def sample_fdp_rdf():
    """Sample FDP RDF data."""
    return """
    @prefix dcat: <http://www.w3.org/ns/dcat#> .
    @prefix dct: <http://purl.org/dc/terms/> .
    @prefix ldp: <http://www.w3.org/ns/ldp#> .
    @prefix fdp: <https://w3id.org/fdp/fdp-o#> .

    <https://example.org/fdp>
        a fdp:FairDataPoint, dcat:DataService ;
        dct:title "Test FDP" ;
        dct:description "A test FAIR Data Point" ;
        dct:publisher "Test Publisher" ;
        ldp:contains <https://example.org/catalog/1> .
    """


@pytest.fixture
def sample_catalog_rdf():
    """Sample Catalog RDF data."""
    return """
    @prefix dcat: <http://www.w3.org/ns/dcat#> .
    @prefix dct: <http://purl.org/dc/terms/> .

    <https://example.org/catalog/1>
        a dcat:Catalog ;
        dct:title "Test Catalog" ;
        dct:description "A test catalog" ;
        dcat:dataset <https://example.org/dataset/1> .
    """


@pytest.fixture
def sample_dataset_rdf():
    """Sample Dataset RDF data."""
    return """
    @prefix dcat: <http://www.w3.org/ns/dcat#> .
    @prefix dct: <http://purl.org/dc/terms/> .
    @prefix vcard: <http://www.w3.org/2006/vcard/ns#> .

    <https://example.org/dataset/1>
        a dcat:Dataset ;
        dct:title "Test Dataset" ;
        dct:description "A test dataset for integration testing" ;
        dct:publisher "Test Publisher" ;
        dcat:keyword "test", "integration" ;
        dcat:contactPoint [
            a vcard:Kind ;
            vcard:fn "Test Contact" ;
            vcard:hasEmail <mailto:test@example.org>
        ] .
    """


class TestIndexRoute:
    """Test the landing page."""

    def test_index_page_loads(self, client):
        """Test that the index page loads successfully."""
        response = client.get('/')
        assert response.status_code == 200
        assert b'Humanitarian Data Space' in response.data

    def test_index_shows_status(self, client):
        """Test that the index page shows status counts."""
        response = client.get('/')
        assert b'FDPs Configured' in response.data
        assert b'Datasets in Basket' in response.data


class TestFDPRoutes:
    """Test FDP management routes."""

    def test_list_fdps_empty(self, client):
        """Test listing FDPs when none configured."""
        response = client.get('/fdp/')
        assert response.status_code == 200
        assert b'No FDPs configured' in response.data

    def test_add_fdp_form(self, client):
        """Test the add FDP form page."""
        response = client.get('/fdp/add')
        assert response.status_code == 200
        assert b'Add FAIR Data Point' in response.data

    @responses.activate
    def test_add_fdp_success(self, client, sample_fdp_rdf):
        """Test adding an FDP successfully."""
        responses.add(
            responses.GET,
            'https://example.org/fdp',
            body=sample_fdp_rdf,
            content_type='text/turtle',
        )

        response = client.post('/fdp/add', data={
            'url': 'https://example.org/fdp',
        }, follow_redirects=True)

        assert response.status_code == 200
        assert b'Test FDP' in response.data or b'Successfully added' in response.data

    def test_add_fdp_invalid_url(self, client):
        """Test adding an FDP with invalid URL."""
        response = client.post('/fdp/add', data={
            'url': 'not-a-valid-url',
        })

        assert response.status_code == 200
        assert b'must start with http' in response.data


class TestDatasetRoutes:
    """Test dataset browsing routes."""

    def test_browse_datasets_empty(self, client):
        """Test browsing datasets when none available."""
        response = client.get('/datasets/')
        assert response.status_code == 200
        assert b'No datasets available' in response.data

    def test_browse_with_filters(self, client):
        """Test browsing datasets with filters."""
        response = client.get('/datasets/?q=test&sort=title')
        assert response.status_code == 200

    def test_refresh_without_fdps(self, client):
        """Test refresh without FDPs configured."""
        response = client.post('/datasets/refresh', follow_redirects=True)
        assert response.status_code == 200
        assert b'No FDPs configured' in response.data


class TestRequestRoutes:
    """Test request composition routes."""

    def test_basket_empty(self, client):
        """Test viewing empty basket."""
        response = client.get('/request/')
        assert response.status_code == 200
        assert b'Your basket is empty' in response.data

    def test_compose_without_basket(self, client):
        """Test composing request without items in basket."""
        response = client.get('/request/compose', follow_redirects=True)
        assert response.status_code == 200
        assert b'Your basket is empty' in response.data or b'Browse Datasets' in response.data

    def test_preview_without_request(self, client):
        """Test preview without composed request."""
        response = client.get('/request/preview', follow_redirects=True)
        assert response.status_code == 200

    def test_clear_basket(self, client):
        """Test clearing the basket."""
        with client.session_transaction() as sess:
            sess['basket'] = [{'uri': 'test', 'title': 'Test', 'fdp_title': 'FDP'}]

        response = client.post('/request/clear', follow_redirects=True)
        assert response.status_code == 200
        assert b'Basket cleared' in response.data


class TestFullWorkflow:
    """Test the complete workflow from adding FDP to composing request."""

    @responses.activate
    def test_complete_workflow(
        self, client, sample_fdp_rdf, sample_catalog_rdf, sample_dataset_rdf
    ):
        """Test adding FDP, browsing datasets, and composing request."""
        # Mock FDP, catalog, and dataset endpoints
        responses.add(
            responses.GET,
            'https://example.org/fdp',
            body=sample_fdp_rdf,
            content_type='text/turtle',
        )
        responses.add(
            responses.GET,
            'https://example.org/catalog/1',
            body=sample_catalog_rdf,
            content_type='text/turtle',
        )
        responses.add(
            responses.GET,
            'https://example.org/dataset/1',
            body=sample_dataset_rdf,
            content_type='text/turtle',
        )

        # Step 1: Add FDP
        response = client.post('/fdp/add', data={
            'url': 'https://example.org/fdp',
        }, follow_redirects=True)
        assert response.status_code == 200

        # Step 2: Browse datasets (refresh to fetch)
        responses.add(
            responses.GET,
            'https://example.org/fdp',
            body=sample_fdp_rdf,
            content_type='text/turtle',
        )
        responses.add(
            responses.GET,
            'https://example.org/catalog/1',
            body=sample_catalog_rdf,
            content_type='text/turtle',
        )
        responses.add(
            responses.GET,
            'https://example.org/dataset/1',
            body=sample_dataset_rdf,
            content_type='text/turtle',
        )

        response = client.post('/datasets/refresh', follow_redirects=True)
        assert response.status_code == 200

        # Step 3: Add to basket (simulate)
        with client.session_transaction() as sess:
            sess['basket'] = [{
                'uri': 'https://example.org/dataset/1',
                'uri_hash': 'test123',
                'title': 'Test Dataset',
                'fdp_title': 'Test FDP',
                'contact_point': {'email': 'test@example.org'},
            }]

        # Step 4: View basket
        response = client.get('/request/')
        assert response.status_code == 200
        assert b'Test Dataset' in response.data

        # Step 5: Go to compose form
        response = client.get('/request/compose')
        assert response.status_code == 200
        assert b'Compose' in response.data

        # Step 6: Submit compose form
        response = client.post('/request/compose', data={
            'name': 'Test User',
            'email': 'user@example.org',
            'affiliation': 'Test University',
            'query': 'SELECT * FROM data WHERE x > 10',
            'purpose': 'Research purposes',
        }, follow_redirects=True)
        assert response.status_code == 200

        # Step 7: View preview
        response = client.get('/request/preview')
        assert response.status_code == 200 or response.status_code == 302


class TestSessionPersistence:
    """Test session data persistence across requests."""

    def test_fdp_persists_in_session(self, client):
        """Test that FDP data persists in session."""
        with client.session_transaction() as sess:
            sess['fdps'] = {'hash123': {'uri': 'https://example.org', 'title': 'Test'}}

        response = client.get('/fdp/')
        assert response.status_code == 200
        assert b'Test' in response.data

    def test_basket_persists_in_session(self, client):
        """Test that basket data persists in session."""
        with client.session_transaction() as sess:
            sess['basket'] = [{'uri': 'test', 'uri_hash': 'hash', 'title': 'Test Dataset', 'fdp_title': 'FDP'}]

        response = client.get('/request/')
        assert response.status_code == 200
        assert b'Test Dataset' in response.data


class TestErrorHandling:
    """Test error handling across routes."""

    def test_invalid_dataset_hash(self, client):
        """Test accessing dataset with invalid hash."""
        response = client.get('/datasets/nonexistent123', follow_redirects=True)
        assert response.status_code == 200
        assert b'not found' in response.data.lower() or b'browse' in response.data.lower()

    def test_remove_nonexistent_from_basket(self, client):
        """Test removing nonexistent item from basket."""
        response = client.post('/datasets/nonexistent/remove-from-basket', follow_redirects=True)
        assert response.status_code == 200
