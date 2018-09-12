from harvest import Item, logger
from opensearch_harvester import OpenSearchHarvester


class ProbaVHarvester(OpenSearchHarvester):
    def __init__(self, config):
        super().__init__(config)
        self.end_date = config.get("end_date", "")

    def _get_start_url(self, start_date):
        logger.debug(f"Start date: {start_date}")

        return self.url_template.format(start_date=start_date, end_date=self.end_date)

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
        logger.debug(f"Found {self._get_ingestiondate(entry)}")
        return Item(
            harvester=self.name,
            source=self.source,
            identifier=self._get_identifier(entry),
            source_date=self._get_ingestiondate(entry),
            content=entry.encode(),
        )

    def _get_identifier(self, entry):
        """Get the product's identifier, i.e., the id used by the community."""
        full_identifier = entry.find("identifier").text
        identifier_parts = full_identifier.split(":")

        return "{}_{}".format(identifier_parts[-2], identifier_parts[-1])

    def _get_ingestiondate(self, entry):
        """
        Get the product's start date.

        VITO sorts the search results by start date. While it's possible to
        sort by VITO's version of ingestion date, or publication date, etc.,
        the results are always sorted by start date. This poses a problem,
        because an older product may be added to the collection after a newer
        product.
        (the date that the product was added to the source db).
        """
        return entry.find("date", {"name": "ingestiondate"}).text
