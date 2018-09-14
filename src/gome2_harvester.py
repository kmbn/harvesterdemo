from datetime import datetime, timedelta
from calendar import monthrange
from requests import get
from tenacity import retry, stop_after_attempt, wait_fixed

from harvest import BaseHarvester, Item, logger


class Gome2Harvester(BaseHarvester):
    def __init__(self, config):
        super(Gome2Harvester, self).__init__(config)
        self.coverage = config["coverage"]
        self.today = datetime.utcnow().strftime("%Y-%m-%d")

    def _get_start_url(self, start_date):
        logger.debug("Start date: {}".format(start_date))
        logger.debug(self.start_date)

        print(self.url_template.format(start_date=start_date, coverage=self.coverage))
        return self.url_template.format(start_date=start_date, coverage=self.coverage)

    def _harvest(self, url):
        logger.debug(self.start_date)
        logger.debug("fetching with {}".format(url))
        missing_dates_response = self.get_missing_dates_response(url)
        missing_dates_set = self.make_missing_dates_set(missing_dates_response)
        dates_in_month = self.get_dates_in_month()
        entries = self.get_dates_with_products(missing_dates_set, dates_in_month)
        self.start_date = self.get_new_start_date(dates_in_month)
        url = self.make_next_url()

        return (entries, url)

    @retry(wait=wait_fixed(60), stop=stop_after_attempt(3))
    def get_missing_dates_response(self, url):
        """Check for missing dates and raise an exception if the response is not 200."""
        logger.debug("Fetching...")
        r = get(url, timeout=self.config.get("timeout", 10))
        r.raise_for_status()

        return r.json()

    def make_missing_dates_set(self, missing_dates):
        return {missing_date["missingDate"] for missing_date in missing_dates}

    def get_dates_in_month(self):
        year = int(self.start_date[:4])
        print(year)
        month = int(self.start_date[5:7])
        print(month)

        if self.start_date[:7] < self.today[:7]:
            # Take the whole month
            num_days = monthrange(year, month)[1]
        else:
            # Take only up to today
            num_days = int(self.today[8:])

        return [
            datetime(year, month, day).strftime("%Y-%m-%d")
            for day in range(1, num_days + 1)
        ]

    def get_dates_with_products(self, missing_dates_set, dates_in_month):
        return [date for date in dates_in_month if date not in missing_dates_set]

    def get_new_start_date(self, dates_in_month):
        last_date_of_query_month = dates_in_month[-1]
        print(self.today)
        print(last_date_of_query_month)

        if last_date_of_query_month < self.today:
            return self.make_new_start_date(last_date_of_query_month)
        else:
            return None

    def make_new_start_date(self, last_date_of_query_month):
        new_datetime = datetime.strptime(
            last_date_of_query_month, "%Y-%m-%d"
        ) + timedelta(days=1)

        return new_datetime.strftime("%Y-%m-%d")

    def make_next_url(self):
        if self.start_date:
            return self._get_start_url(self.start_date)
        else:
            return None

    def _convert(self, entry):
        """Convert an OpenSearch entry into an item that can be saved in the db."""
        logger.debug("Found {}_{}".format(self.coverage, entry))
        logger.debug("Found {}".format(entry))
        identifier = "{}_{}".format(self.coverage, entry)
        return Item(
            harvester=self.name,
            source=self.source,
            identifier=identifier,
            source_date=entry,
            content=None,
        )
