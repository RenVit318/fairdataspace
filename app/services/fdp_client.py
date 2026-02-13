"""FDP Client for fetching and parsing FAIR Data Point metadata."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, List

import requests
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS

from app.models import FairDataPoint, Dataset, ContactPoint, Distribution


logger = logging.getLogger(__name__)


# RDF Namespaces
DCAT = Namespace('http://www.w3.org/ns/dcat#')
DCT = Namespace('http://purl.org/dc/terms/')
FOAF = Namespace('http://xmlns.com/foaf/0.1/')
VCARD = Namespace('http://www.w3.org/2006/vcard/ns#')
FDP = Namespace('https://w3id.org/fdp/fdp-o#')
LDP = Namespace('http://www.w3.org/ns/ldp#')
VOID = Namespace('http://rdfs.org/ns/void#')


class FDPError(Exception):
    """Base exception for FDP-related errors."""
    pass


class FDPConnectionError(FDPError):
    """Raised when an FDP cannot be reached."""
    pass


class FDPParseError(FDPError):
    """Raised when FDP RDF cannot be parsed."""
    pass


class FDPTimeoutError(FDPError):
    """Raised when FDP request times out."""
    pass


class FDPClient:
    """Client for fetching and parsing FAIR Data Point metadata."""

    def __init__(self, timeout: int = 30, verify_ssl: bool = True):
        """
        Initialize the FDP client.

        Args:
            timeout: Request timeout in seconds.
            verify_ssl: Whether to verify SSL certificates.
        """
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self._headers = {
            'Accept': 'text/turtle, application/ld+json;q=0.9, application/rdf+xml;q=0.8'
        }
        if not verify_ssl:
            # Suppress InsecureRequestWarning when SSL verification is disabled
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _fetch_rdf(self, uri: str) -> Graph:
        """
        Fetch RDF content from a URI and parse it into a Graph.

        Args:
            uri: The URI to fetch RDF from.

        Returns:
            An rdflib Graph containing the parsed RDF.

        Raises:
            FDPConnectionError: If the URI cannot be reached.
            FDPTimeoutError: If the request times out.
            FDPParseError: If the RDF cannot be parsed.
        """
        try:
            response = requests.get(
                uri, headers=self._headers, timeout=self.timeout, verify=self.verify_ssl
            )
            response.raise_for_status()
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout fetching {uri}: {e}")
            raise FDPTimeoutError(f"Request to {uri} timed out") from e
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error fetching {uri}: {e}")
            raise FDPConnectionError(f"Could not connect to {uri}") from e
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching {uri}: {e}")
            raise FDPConnectionError(f"HTTP error for {uri}: {e.response.status_code}") from e
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching {uri}: {e}")
            raise FDPConnectionError(f"Request failed for {uri}") from e

        # Determine format from content-type
        content_type = response.headers.get('Content-Type', '')
        if 'turtle' in content_type:
            rdf_format = 'turtle'
        elif 'json' in content_type:
            rdf_format = 'json-ld'
        elif 'xml' in content_type:
            rdf_format = 'xml'
        else:
            # Default to turtle
            rdf_format = 'turtle'

        try:
            graph = Graph()
            graph.parse(data=response.text, format=rdf_format)
            return graph
        except Exception as e:
            logger.error(f"Parse error for {uri}: {e}")
            raise FDPParseError(f"Could not parse RDF from {uri}") from e

    def _get_literal_value(
        self, graph: Graph, subject: URIRef, predicate: URIRef
    ) -> Optional[str]:
        """Extract a literal value from the graph."""
        for obj in graph.objects(subject, predicate):
            if isinstance(obj, Literal):
                return str(obj)
            elif isinstance(obj, URIRef):
                # Try to get label for URI
                for label in graph.objects(obj, RDFS.label):
                    return str(label)
                # Or get foaf:name
                for name in graph.objects(obj, FOAF.name):
                    return str(name)
        return None

    def _get_uri_list(
        self, graph: Graph, subject: URIRef, predicate: URIRef
    ) -> List[str]:
        """Extract a list of URI values from the graph."""
        uris = []
        for obj in graph.objects(subject, predicate):
            if isinstance(obj, URIRef):
                uris.append(str(obj))
        return uris

    def _extract_contact_point(
        self, graph: Graph, dataset_uri: URIRef
    ) -> Optional[ContactPoint]:
        """
        Extract contact point information from a dataset.

        Args:
            graph: The RDF graph containing the dataset.
            dataset_uri: The URI of the dataset.

        Returns:
            A ContactPoint instance if found, None otherwise.
        """
        for contact_node in graph.objects(dataset_uri, DCAT.contactPoint):
            name = None
            email = None
            url = None

            # Get name (vcard:fn)
            for fn in graph.objects(contact_node, VCARD.fn):
                name = str(fn)
                break

            # Get email (vcard:hasEmail)
            for email_node in graph.objects(contact_node, VCARD.hasEmail):
                email_str = str(email_node)
                # Handle mailto: URIs
                if email_str.startswith('mailto:'):
                    email = email_str[7:]
                else:
                    email = email_str
                break

            # Get URL (vcard:hasURL)
            for url_node in graph.objects(contact_node, VCARD.hasURL):
                url = str(url_node)
                break

            if name or email or url:
                return ContactPoint(name=name, email=email, url=url)

        return None

    def _parse_date(self, value: Optional[str]) -> Optional[datetime]:
        """Parse a date string to datetime."""
        if not value:
            return None
        try:
            # Try ISO format first
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            pass
        try:
            # Try date only format
            return datetime.strptime(value, '%Y-%m-%d')
        except ValueError:
            pass
        return None

    def fetch_fdp(self, uri: str) -> FairDataPoint:
        """
        Fetch and parse FDP metadata.

        Args:
            uri: The FDP endpoint URL.

        Returns:
            FairDataPoint with metadata and catalog URIs.

        Raises:
            FDPConnectionError: If FDP is unreachable.
            FDPParseError: If RDF cannot be parsed.
        """
        graph = self._fetch_rdf(uri)
        # Try both with and without trailing slash for URI matching
        # FDPs may use either form in their RDF data
        normalized_uri = uri.rstrip('/')
        fdp_uri = URIRef(normalized_uri)
        fdp_uri_with_slash = URIRef(normalized_uri + '/')

        # Get title - try both URI forms
        title = self._get_literal_value(graph, fdp_uri, DCT.title)
        if not title:
            title = self._get_literal_value(graph, fdp_uri_with_slash, DCT.title)
        if not title:
            title = self._get_literal_value(graph, fdp_uri, RDFS.label)
        if not title:
            title = self._get_literal_value(graph, fdp_uri_with_slash, RDFS.label)
        if not title:
            title = uri

        # Get description - try both URI forms
        description = self._get_literal_value(graph, fdp_uri, DCT.description)
        if not description:
            description = self._get_literal_value(graph, fdp_uri_with_slash, DCT.description)

        # Get publisher - try both URI forms
        publisher = self._get_literal_value(graph, fdp_uri, DCT.publisher)
        if not publisher:
            publisher = self._get_literal_value(graph, fdp_uri_with_slash, DCT.publisher)

        # Get catalogs (via fdp:metadataCatalog or ldp:DirectContainer)
        # Try both URI forms
        catalogs = self._get_uri_list(graph, fdp_uri, FDP.metadataCatalog)
        if not catalogs:
            catalogs = self._get_uri_list(graph, fdp_uri_with_slash, FDP.metadataCatalog)
        logger.info(f"Found {len(catalogs)} catalogs via fdp:metadataCatalog for {uri}")

        # Also check for catalogs in LDP DirectContainer
        # Look for any ldp:DirectContainer that has this FDP as membershipResource
        for container in graph.subjects(RDF.type, LDP.DirectContainer):
            membership_resource = graph.value(container, LDP.membershipResource)
            # Check both URI forms
            if membership_resource == fdp_uri or membership_resource == fdp_uri_with_slash:
                # Get all catalogs from ldp:contains
                ldp_catalogs = self._get_uri_list(graph, container, LDP.contains)
                logger.info(f"Found {len(ldp_catalogs)} catalogs via LDP DirectContainer for {uri}")
                # Add any new catalogs not already in the list
                for cat in ldp_catalogs:
                    if cat not in catalogs:
                        catalogs.append(cat)

        logger.info(f"Total catalogs discovered for {uri}: {len(catalogs)}")

        # Check if this is an index FDP (has fdp:metadataService links)
        # Try both URI forms
        linked_fdps = self._get_uri_list(graph, fdp_uri, FDP.metadataService)
        if not linked_fdps:
            linked_fdps = self._get_uri_list(graph, fdp_uri_with_slash, FDP.metadataService)
        is_index = len(linked_fdps) > 0

        return FairDataPoint(
            uri=normalized_uri,  # Return normalized URI (without trailing slash)
            title=title,
            description=description,
            publisher=publisher,
            is_index=is_index,
            catalogs=catalogs,
            linked_fdps=linked_fdps,
            last_fetched=datetime.now(),
            status='active',
        )

    def fetch_catalog_with_datasets(
        self, catalog_uri: str, fdp_uri: str, fdp_title: str
    ) -> List[Dataset]:
        """
        Fetch catalog and extract dataset summaries from the catalog's RDF graph.
        This is much faster than fetching each dataset individually.

        Args:
            catalog_uri: The catalog URI.
            fdp_uri: Parent FDP URI.
            fdp_title: Parent FDP title.

        Returns:
            List of Dataset objects with metadata extracted from catalog.
        """
        graph = self._fetch_rdf(catalog_uri)
        datasets = []

        # Get catalog title for context
        catalog_uri_ref = URIRef(catalog_uri)
        catalog_title = self._get_literal_value(graph, catalog_uri_ref, DCT.title)
        if not catalog_title:
            catalog_title = self._get_literal_value(graph, catalog_uri_ref, RDFS.label)
        if not catalog_title:
            catalog_title = catalog_uri.split('/')[-1]  # Use last part of URI as fallback

        # Find all datasets in the catalog
        dataset_uris = set()

        # Method 1: dcat:dataset
        for ds_uri in graph.objects(catalog_uri_ref, DCAT.dataset):
            dataset_uris.add(str(ds_uri))

        # Method 2: LDP DirectContainer
        for container in graph.subjects(RDF.type, LDP.DirectContainer):
            membership_resource = graph.value(container, LDP.membershipResource)
            if membership_resource == catalog_uri_ref:
                for ds_uri in graph.objects(container, LDP.contains):
                    dataset_uris.add(str(ds_uri))

        # Extract metadata for each dataset from the catalog graph
        needs_fetch = []
        for ds_uri_str in dataset_uris:
            ds_uri = URIRef(ds_uri_str)

            # Try to extract metadata from the catalog graph (inline)
            title = self._get_literal_value(graph, ds_uri, DCT.title)

            if title:
                # Catalog has inline metadata — use it
                description = self._get_literal_value(graph, ds_uri, DCT.description)
                publisher = self._get_literal_value(graph, ds_uri, DCT.publisher)
                creator = self._get_literal_value(graph, ds_uri, DCT.creator)
                themes = self._get_uri_list(graph, ds_uri, DCAT.theme)
                keywords = []
                for kw in graph.objects(ds_uri, DCAT.keyword):
                    keywords.append(str(kw))
                contact_point = self._extract_contact_point(graph, ds_uri)
                landing_page = self._get_literal_value(graph, ds_uri, DCAT.landingPage)

                dataset = Dataset(
                    uri=ds_uri_str,
                    title=title,
                    catalog_uri=catalog_uri,
                    catalog_title=catalog_title,
                    fdp_uri=fdp_uri,
                    fdp_title=fdp_title,
                    description=description,
                    publisher=publisher,
                    creator=creator,
                    themes=themes,
                    keywords=keywords,
                    contact_point=contact_point,
                    landing_page=landing_page,
                )
                datasets.append(dataset)
            else:
                needs_fetch.append(ds_uri_str)

        # Fetch datasets without inline metadata concurrently
        if needs_fetch:
            logger.info(f"Fetching {len(needs_fetch)} datasets individually (no inline metadata)")

            def _fetch_one(uri: str) -> Dataset:
                try:
                    ds = self.fetch_dataset(uri, catalog_uri, fdp_uri, fdp_title)
                    ds.catalog_title = catalog_title
                    return ds
                except FDPError as e:
                    logger.warning(f"Failed to fetch dataset {uri}: {e}")
                    return Dataset(
                        uri=uri, title=uri,
                        catalog_uri=catalog_uri, catalog_title=catalog_title,
                        fdp_uri=fdp_uri, fdp_title=fdp_title,
                    )

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = {pool.submit(_fetch_one, uri): uri for uri in needs_fetch}
                for future in as_completed(futures):
                    datasets.append(future.result())

        logger.info(f"Extracted {len(datasets)} datasets from catalog {catalog_uri}")
        return datasets

    def fetch_dataset(
        self, uri: str, catalog_uri: str, fdp_uri: str, fdp_title: str
    ) -> Dataset:
        """
        Fetch and parse dataset metadata.

        Args:
            uri: The dataset URI.
            catalog_uri: Parent catalog URI.
            fdp_uri: Parent FDP URI.
            fdp_title: Parent FDP title for display.

        Returns:
            Dataset with full metadata.

        Raises:
            FDPConnectionError: If dataset is unreachable.
            FDPParseError: If RDF cannot be parsed.
        """
        graph = self._fetch_rdf(uri)
        dataset_uri = URIRef(uri)

        # Get title
        title = self._get_literal_value(graph, dataset_uri, DCT.title)
        if not title:
            title = self._get_literal_value(graph, dataset_uri, RDFS.label)
        if not title:
            title = uri

        # Get description
        description = self._get_literal_value(graph, dataset_uri, DCT.description)

        # Get publisher
        publisher = self._get_literal_value(graph, dataset_uri, DCT.publisher)

        # Get creator
        creator = self._get_literal_value(graph, dataset_uri, DCT.creator)

        # Get dates
        issued_str = self._get_literal_value(graph, dataset_uri, DCT.issued)
        issued = self._parse_date(issued_str)

        modified_str = self._get_literal_value(graph, dataset_uri, DCT.modified)
        modified = self._parse_date(modified_str)

        # Get themes
        themes = self._get_uri_list(graph, dataset_uri, DCAT.theme)

        # Get theme labels
        theme_labels = []
        for theme_uri in themes:
            label = self._get_literal_value(graph, URIRef(theme_uri), RDFS.label)
            if label:
                theme_labels.append(label)

        # Get keywords
        keywords = []
        for keyword in graph.objects(dataset_uri, DCAT.keyword):
            keywords.append(str(keyword))

        # Get contact point
        contact_point = self._extract_contact_point(graph, dataset_uri)

        # Get landing page
        landing_page = None
        for lp in graph.objects(dataset_uri, DCAT.landingPage):
            landing_page = str(lp)
            break

        # Get distributions - fetch full metadata for each
        distribution_uris = self._get_uri_list(graph, dataset_uri, DCAT.distribution)
        distributions = []
        for dist_uri in distribution_uris:
            # First try to extract from the same graph (inline distributions)
            dist = self.fetch_distribution(dist_uri, graph=graph)
            # If we only got a URI back (no metadata), try fetching separately
            if not dist.title and not dist.access_url and not dist.endpoint_url:
                try:
                    dist = self.fetch_distribution(dist_uri)
                except Exception as e:
                    logger.warning(f"Could not fetch distribution {dist_uri}: {e}")
            distributions.append(dist)

        return Dataset(
            uri=uri,
            title=title,
            description=description,
            publisher=publisher,
            creator=creator,
            issued=issued,
            modified=modified,
            themes=themes,
            theme_labels=theme_labels,
            keywords=keywords,
            contact_point=contact_point,
            landing_page=landing_page,
            catalog_uri=catalog_uri,
            fdp_uri=fdp_uri,
            fdp_title=fdp_title,
            distributions=distributions,
        )

    def _is_sparql_endpoint(self, graph: Graph, node: URIRef) -> bool:
        """Check if a resource is a SPARQL endpoint based on RDF type or properties."""
        # Check rdf:type for dcat:DataService
        for rdf_type in graph.objects(node, RDF.type):
            type_str = str(rdf_type)
            if 'DataService' in type_str:
                return True

        # Check for void:sparqlEndpoint
        for _ in graph.objects(node, VOID.sparqlEndpoint):
            return True

        # Check for dcat:endpointURL (strong signal)
        for _ in graph.objects(node, DCAT.endpointURL):
            return True

        # Heuristic: check if endpoint URL or access URL contains sparql-like patterns
        for prop in [DCAT.endpointURL, DCAT.accessURL]:
            for url_node in graph.objects(node, prop):
                url_str = str(url_node).lower()
                if any(hint in url_str for hint in ['sparql', 'repositories', 'allegrograph']):
                    return True

        return False

    def _extract_endpoint_url(self, graph: Graph, node: URIRef) -> Optional[str]:
        """Extract endpoint URL from a distribution or data service."""
        # Try dcat:endpointURL first (most specific)
        for url in graph.objects(node, DCAT.endpointURL):
            return str(url)

        # Try void:sparqlEndpoint
        for url in graph.objects(node, VOID.sparqlEndpoint):
            return str(url)

        # Check if this distribution links to a dcat:accessService with an endpoint
        for service in graph.objects(node, DCAT.accessService):
            for url in graph.objects(service, DCAT.endpointURL):
                return str(url)

        # Fallback: use accessURL if it looks like a SPARQL endpoint
        for url in graph.objects(node, DCAT.accessURL):
            url_str = str(url).lower()
            if any(hint in url_str for hint in ['sparql', 'repositories', 'allegrograph']):
                return str(url)

        return None

    def fetch_distribution(self, uri: str, graph: Optional[Graph] = None) -> Distribution:
        """
        Fetch and parse a single distribution's metadata.

        Args:
            uri: The distribution URI.
            graph: Optional pre-fetched graph containing this distribution's data.

        Returns:
            Distribution with full metadata.
        """
        if graph is None:
            try:
                graph = self._fetch_rdf(uri)
            except FDPError:
                # Return minimal distribution if fetch fails
                return Distribution(uri=uri)

        dist_uri = URIRef(uri)

        title = self._get_literal_value(graph, dist_uri, DCT.title)
        if not title:
            title = self._get_literal_value(graph, dist_uri, RDFS.label)

        description = self._get_literal_value(graph, dist_uri, DCT.description)

        # Access URLs
        access_url = None
        for url in graph.objects(dist_uri, DCAT.accessURL):
            access_url = str(url)
            break

        download_url = None
        for url in graph.objects(dist_uri, DCAT.downloadURL):
            download_url = str(url)
            break

        # Media type and format
        media_type = self._get_literal_value(graph, dist_uri, DCAT.mediaType)
        dist_format = self._get_literal_value(graph, dist_uri, DCT['format'])

        # Byte size
        byte_size = None
        size_val = self._get_literal_value(graph, dist_uri, DCAT.byteSize)
        if size_val:
            try:
                byte_size = int(size_val)
            except (ValueError, TypeError):
                pass

        # SPARQL endpoint detection
        endpoint_url = self._extract_endpoint_url(graph, dist_uri)
        endpoint_description = self._get_literal_value(
            graph, dist_uri, DCAT.endpointDescription
        )
        is_sparql = self._is_sparql_endpoint(graph, dist_uri)

        # If we found an endpoint URL but didn't detect SPARQL, mark it anyway
        if endpoint_url and not is_sparql:
            is_sparql = True

        # Contact point at distribution level
        contact_point = self._extract_contact_point(graph, dist_uri)

        return Distribution(
            uri=uri,
            title=title,
            description=description,
            access_url=access_url,
            download_url=download_url,
            media_type=media_type,
            format=dist_format,
            byte_size=byte_size,
            endpoint_url=endpoint_url,
            endpoint_description=endpoint_description,
            is_sparql_endpoint=is_sparql,
            contact_point=contact_point,
        )

    def fetch_all_from_index(self, index_uri: str) -> List[FairDataPoint]:
        """
        Fetch the index FDP and all linked FDPs.

        Fetches the index, discovers linked FDPs, and fetches each one.
        Errors for individual FDPs are logged but don't stop processing.

        Args:
            index_uri: URI of the index FDP.

        Returns:
            List of FairDataPoint instances (index + all successfully fetched linked FDPs).

        Raises:
            FDPConnectionError: If index FDP is unreachable.
            FDPParseError: If index FDP cannot be parsed.
        """
        # First fetch the index itself
        index_fdp = self.fetch_fdp(index_uri)
        result = [index_fdp]

        # Then fetch all linked FDPs
        for linked_uri in index_fdp.linked_fdps:
            try:
                linked_fdp = self.fetch_fdp(linked_uri)
                result.append(linked_fdp)
                logger.info(f"Successfully fetched linked FDP: {linked_uri}")
            except FDPError as e:
                # Log error but continue with other FDPs
                logger.warning(f"Failed to fetch linked FDP {linked_uri}: {e}")
                # Add a placeholder FDP with error status
                error_fdp = FairDataPoint(
                    uri=linked_uri,
                    title=linked_uri,
                    is_index=False,
                    status='error',
                    error_message=str(e),
                )
                result.append(error_fdp)

        return result
