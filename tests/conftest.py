"""Pytest fixtures for the Data Visiting PoC tests."""

import pytest
from pathlib import Path

# Import create_app conditionally (not available until Subtask 8)
try:
    from app import create_app
    HAS_APP_FACTORY = True
except ImportError:
    HAS_APP_FACTORY = False

from app.models import (
    FairDataPoint,
    Catalog,
    Dataset,
    ContactPoint,
    DataRequest,
    DatasetReference,
)


FIXTURES_DIR = Path(__file__).parent / 'fixtures'


@pytest.fixture
def app():
    """Create and configure a Flask app instance for testing."""
    if not HAS_APP_FACTORY:
        pytest.skip("Flask app factory not yet implemented")
    app = create_app({'TESTING': True, 'SECRET_KEY': 'test-secret-key', 'DEFAULT_FDPS': []})
    yield app


@pytest.fixture
def client(app):
    """Create a test client for the Flask app."""
    if not HAS_APP_FACTORY:
        pytest.skip("Flask app factory not yet implemented")
    return app.test_client()


@pytest.fixture
def sample_fdp_root_rdf() -> str:
    """Load the sample FDP root RDF fixture."""
    with open(FIXTURES_DIR / 'fdp_root.ttl', 'r') as f:
        return f.read()


@pytest.fixture
def sample_catalog_rdf() -> str:
    """Load the sample catalog RDF fixture."""
    with open(FIXTURES_DIR / 'fdp_catalog.ttl', 'r') as f:
        return f.read()


@pytest.fixture
def sample_dataset_rdf() -> str:
    """Load the sample dataset RDF fixture."""
    with open(FIXTURES_DIR / 'dataset.ttl', 'r') as f:
        return f.read()


@pytest.fixture
def sample_fdp_index_rdf() -> str:
    """Load the sample FDP index RDF fixture."""
    with open(FIXTURES_DIR / 'fdp_index.ttl', 'r') as f:
        return f.read()


@pytest.fixture
def sample_fdp() -> FairDataPoint:
    """Create a sample FairDataPoint instance for testing."""
    return FairDataPoint(
        uri='https://example.org/fdp',
        title='Example FAIR Data Point',
        description='A sample FAIR Data Point for testing purposes.',
        publisher='Example University',
        is_index=False,
        catalogs=['https://example.org/fdp/catalog/research-data'],
        linked_fdps=[],
        status='active',
    )


@pytest.fixture
def sample_catalog() -> Catalog:
    """Create a sample Catalog instance for testing."""
    return Catalog(
        uri='https://example.org/fdp/catalog/research-data',
        title='Research Data Catalog',
        description='Catalog containing research datasets.',
        publisher='Example University',
        fdp_uri='https://example.org/fdp',
        datasets=[
            'https://example.org/fdp/dataset/biodiversity-2023',
            'https://example.org/fdp/dataset/climate-observations',
        ],
        themes=['http://www.wikidata.org/entity/Q7150'],
    )


@pytest.fixture
def sample_contact_point() -> ContactPoint:
    """Create a sample ContactPoint instance for testing."""
    return ContactPoint(
        name='Research Data Team',
        email='data-requests@example.org',
    )


@pytest.fixture
def sample_dataset(sample_contact_point: ContactPoint) -> Dataset:
    """Create a sample Dataset instance for testing."""
    return Dataset(
        uri='https://example.org/fdp/dataset/biodiversity-2023',
        title='Biodiversity Survey Data 2023',
        description='Annual biodiversity survey results.',
        publisher='Example University',
        creator='Dr. Jane Smith',
        themes=['http://www.wikidata.org/entity/Q47041'],
        theme_labels=['Biodiversity'],
        keywords=['ecology', 'species', 'survey'],
        contact_point=sample_contact_point,
        landing_page='https://example.org/datasets/biodiversity-2023',
        catalog_uri='https://example.org/fdp/catalog/research-data',
        fdp_uri='https://example.org/fdp',
        fdp_title='Example FAIR Data Point',
    )


@pytest.fixture
def sample_dataset_reference() -> DatasetReference:
    """Create a sample DatasetReference instance for testing."""
    return DatasetReference(
        uri='https://example.org/fdp/dataset/biodiversity-2023',
        title='Biodiversity Survey Data 2023',
        contact_email='data-requests@example.org',
        fdp_title='Example FAIR Data Point',
    )


@pytest.fixture
def sample_data_request(sample_dataset_reference: DatasetReference) -> DataRequest:
    """Create a sample DataRequest instance for testing."""
    return DataRequest(
        requester_name='Dr. John Doe',
        requester_email='j.doe@university.edu',
        requester_affiliation='University of Example',
        requester_orcid='0000-0002-1234-5678',
        datasets=[sample_dataset_reference],
        query='SELECT species, count FROM observations WHERE year = 2023',
        purpose='Analysis of species distribution patterns for conservation planning',
        output_constraints='Aggregated results only, minimum cell size of 5',
        timeline='Results needed within 4 weeks',
    )


@pytest.fixture
def multiple_dataset_references() -> list:
    """Create multiple DatasetReference instances with different contacts."""
    return [
        DatasetReference(
            uri='https://example.org/fdp/dataset/biodiversity-2023',
            title='Biodiversity Survey Data 2023',
            contact_email='data-requests@example.org',
            fdp_title='Example FAIR Data Point',
        ),
        DatasetReference(
            uri='https://example.org/fdp/dataset/climate-observations',
            title='Climate Observations Dataset',
            contact_email='data-requests@example.org',
            fdp_title='Example FAIR Data Point',
        ),
        DatasetReference(
            uri='https://other.org/fdp/dataset/genomics',
            title='Genomics Study Data',
            contact_email='genomics@other.org',
            fdp_title='Other FAIR Data Point',
        ),
    ]
