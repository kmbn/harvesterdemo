from bs4 import BeautifulSoup
from requests import Session
from requests.auth import HTTPBasicAuth
from tenacity import retry, stop_after_attempt, wait_fixed

from harvester import BaseHarvester, Item, logger


class DHuSHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)
        self.session = self._make_session()

    def _make_session(self):
        session = Session()
        session.verify = True
        auth = self.config.get("auth")
        if auth:
            session.auth = HTTPBasicAuth(auth["username"], auth["password"])

        return session

    def _get_start_url(self, start_date):
        logger.debug(f"Start date: {start_date}")
        return self.url_template.format(start_date=start_date)

    def _harvest(self, url):
        raw_feed = self._get_feed(url)
        parsed_feed = self._parse_feed(raw_feed)
        entries = self._get_entries(parsed_feed)
        url = self._add_orderby_if_missing(self._get_next_link(parsed_feed))

        return (entries, url)

    @retry(wait=wait_fixed(60), stop=stop_after_attempt(3))
    def _get_feed(self, url):
        """Get the feed and raise an exception if the response is not 200."""
        logger.debug("Fetching...")
        r = self.session.get(url, timeout=self.config.get("timeout", 10))
        r.raise_for_status()

        return r.text

    def _parse_feed(self, xml):
        """Parse the results feed."""
        return BeautifulSoup(xml, "lxml")

    def _get_entries(self, feed):
        """Get all the entries from the feed."""
        return feed.find_all("entry")

    def _get_next_link(self, feed):
        """Get the next link. Return None if there's no next link."""
        next_link = feed.find("link", {"rel": "next", "type": "application/atom+xml"})

        if next_link:
            return next_link["href"]
        else:
            return None

    def _convert(self, entry):
        """Convert an OpenSearch entry into an item that can be saved in the db."""
        logger.debug(f"Found {self._get_identifier(entry)}")
        return Item(
            harvester=self.name,
            source=self.source,
            identifier=self._get_identifier(entry),
            source_date=self._get_ingestiondate(entry),
            content=entry.encode(),
        )

    def _get_identifier(self, entry):
        """Get the product's identifier, i.e., the id used by the community."""
        return entry.find("str", {"name": "identifier"}).text

    def _get_ingestiondate(self, entry):
        """
        Get the product's ingestiondate
        (the date that the product was added to the source db).
        """
        return entry.find("date", {"name": "ingestiondate"}).text

    def _add_orderby_if_missing(self, next_link):
        """
        Add the orderby parameter if it's not part of the next link query.

        DHuS sometimes ignores the orderby query when creating the next link, etc.,
        which causes the sort order to change.

        If we add it back we can keep harvesting normally.
        """
        orderby = "&orderby=ingestiondate asc"
        if next_link and orderby not in next_link:
            next_link = next_link + orderby

        return next_link
