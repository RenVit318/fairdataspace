"""Dataset Service for aggregating and filtering datasets."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any

from app.models import Dataset
from app.services.fdp_client import FDPClient, FDPError


logger = logging.getLogger(__name__)


@dataclass
class Theme:
    """Represents a theme for filtering datasets."""

    uri: str
    label: str
    count: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'uri': self.uri,
            'label': self.label,
            'count': self.count,
        }


class DatasetService:
    """Service for aggregating and filtering datasets from FDPs."""

    def __init__(self, fdp_client: FDPClient):
        """
        Initialize the dataset service.

        Args:
            fdp_client: FDP client for fetching metadata.
        """
        self.fdp_client = fdp_client

    def get_all_datasets(self, fdp_uris: List[str]) -> List[Dataset]:
        """
        Fetch all datasets from the given FDPs.

        Fetches FDPs concurrently, then fetches all discovered catalogs
        concurrently to minimise total wait time.

        Args:
            fdp_uris: List of FDP URIs to fetch from.

        Returns:
            List of all datasets from all FDPs.
        """
        # Step 1: fetch all FDPs concurrently to discover catalogs
        catalog_tasks = []  # list of (catalog_uri, fdp_uri, fdp_title)

        def _fetch_fdp(uri):
            try:
                return self.fdp_client.fetch_fdp(uri)
            except FDPError as e:
                logger.warning(f"Failed to fetch FDP {uri}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_fetch_fdp, uri): uri for uri in fdp_uris}
            for future in as_completed(futures):
                fdp = future.result()
                if fdp:
                    logger.info(f"Fetching datasets from {len(fdp.catalogs)} catalogs in {fdp.title}")
                    for catalog_uri in fdp.catalogs:
                        catalog_tasks.append((catalog_uri, fdp.uri, fdp.title))

        # Step 2: fetch all catalogs concurrently
        datasets = []

        def _fetch_catalog(task):
            catalog_uri, fdp_uri, fdp_title = task
            try:
                return self.fdp_client.fetch_catalog_with_datasets(
                    catalog_uri, fdp_uri, fdp_title
                )
            except FDPError as e:
                logger.warning(f"Failed to fetch catalog {catalog_uri}: {e}")
                return []

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_fetch_catalog, t): t for t in catalog_tasks}
            for future in as_completed(futures):
                datasets.extend(future.result())

        logger.info(f"Total datasets fetched: {len(datasets)}")
        return datasets

    def filter_by_theme(
        self, datasets: List[Dataset], theme_uri: str
    ) -> List[Dataset]:
        """
        Filter datasets by theme URI.

        Args:
            datasets: Datasets to filter.
            theme_uri: Theme URI to filter by.

        Returns:
            Datasets that have the specified theme.
        """
        return [ds for ds in datasets if theme_uri in ds.themes]

    def search(self, datasets: List[Dataset], query: str) -> List[Dataset]:
        """
        Full-text search across dataset metadata.

        Searches across title, description, and keywords.
        Results are ordered by relevance:
        - Title match (highest priority)
        - Description match
        - Keyword match (lowest priority)

        Args:
            datasets: Datasets to search.
            query: Search query string (case-insensitive).

        Returns:
            Matching datasets, ordered by relevance.
        """
        if not query:
            return datasets

        query_lower = query.lower()
        query_terms = query_lower.split()

        scored_results = []

        for ds in datasets:
            score = 0
            title_lower = (ds.title or '').lower()
            desc_lower = (ds.description or '').lower()
            keywords_lower = [kw.lower() for kw in ds.keywords]

            for term in query_terms:
                # Title match (highest weight)
                if term in title_lower:
                    score += 100
                    # Bonus for exact title match
                    if title_lower == term:
                        score += 50

                # Description match
                if term in desc_lower:
                    score += 10

                # Keyword match
                if any(term in kw for kw in keywords_lower):
                    score += 5
                    # Bonus for exact keyword match
                    if term in keywords_lower:
                        score += 10

                # Theme label match
                theme_labels_lower = [tl.lower() for tl in ds.theme_labels]
                if any(term in tl for tl in theme_labels_lower):
                    score += 5

            if score > 0:
                scored_results.append((score, ds))

        # Sort by score (descending), then by title
        scored_results.sort(key=lambda x: (-x[0], x[1].title or ''))

        return [ds for _, ds in scored_results]

    def get_available_themes(self, datasets: List[Dataset]) -> List[Theme]:
        """
        Extract unique themes from datasets for filter UI.

        Args:
            datasets: Datasets to extract themes from.

        Returns:
            List of Theme objects with uri, label, and count.
        """
        theme_counts: Dict[str, Dict[str, Any]] = {}

        for ds in datasets:
            for i, theme_uri in enumerate(ds.themes):
                if theme_uri not in theme_counts:
                    # Try to get a label
                    label = ''
                    if i < len(ds.theme_labels):
                        label = ds.theme_labels[i]
                    if not label:
                        # Use the last part of the URI as a fallback label
                        label = theme_uri.split('/')[-1]

                    theme_counts[theme_uri] = {
                        'uri': theme_uri,
                        'label': label,
                        'count': 0,
                    }

                theme_counts[theme_uri]['count'] += 1

        # Convert to Theme objects and sort by count (descending)
        themes = [
            Theme(
                uri=data['uri'],
                label=data['label'],
                count=data['count'],
            )
            for data in theme_counts.values()
        ]

        themes.sort(key=lambda t: (-t.count, t.label))

        return themes
