from bs4 import BeautifulSoup
from requests import Session
from requests.auth import HTTPBasicAuth
from tenacity import retry, stop_after_attempt, wait_fixed

from harvest import BaseHarvester, logger


class OpenSearchHarvester(BaseHarvester):
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

    def _harvest(self, url):
        raw_feed = self._get_feed(url)
        parsed_feed = self._parse_feed(raw_feed)
        entries = self._get_entries(parsed_feed)
        url = self._get_next_link(parsed_feed)

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
