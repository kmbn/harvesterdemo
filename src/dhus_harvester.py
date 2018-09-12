from harvest import Item, logger
from opensearch_harvester import OpenSearchHarvester


class DHuSHarvester(OpenSearchHarvester):
    def __init__(self, config):
        super().__init__(config)
        self.end_date = config.get("end_date", "NOW")

    def _get_start_url(self, start_date):
        logger.debug(f"Start date: {start_date}")

        return self.url_template.format(start_date=start_date, end_date=self.end_date)

    def _get_next_link(self, feed):
        """Get the next link. Return None if there's no next link."""
        next_link = feed.find("link", {"rel": "next", "type": "application/atom+xml"})

        if next_link:
            return self._add_orderby_if_missing(next_link["href"])
        else:
            return None

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
        return entry.find("str", {"name": "identifier"}).text

    def _get_ingestiondate(self, entry):
        """
        Get the product's ingestiondate
        (the date that the product was added to the source db).
        """
        return entry.find("date", {"name": "ingestiondate"}).text
