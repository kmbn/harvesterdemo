from datetime import datetime
from pathlib import Path
from time import time, sleep

from appdirs import user_data_dir
from bs4 import BeautifulSoup
import click
from logzero import logger
from peewee import (
    CharField,
    DateTimeField,
    fn,
    IntegrityError,
    InternalError,
    OperationalError,
    Model,
    SqliteDatabase,
    TextField,
)
from pid.decorator import pidfile
from requests import Session
from requests.auth import HTTPBasicAuth
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from toml import load as toml


appdir = user_data_dir("Harvester")

config = toml("esa_scihub_sentinel_pre.toml")

HARVESTER_NAME = config["harvester_name"]
SOURCE_NAME = config["source_name"]

DATABASE = SqliteDatabase(None)


session = Session()
session.verify = True
auth = config.get("auth")
if auth:
    session.auth = HTTPBasicAuth(auth["username"], auth["password"])

TIMEOUT = config.get("timeout", 10)


class Item(Model):
    harvester = CharField()
    source = CharField()
    identifier = TextField(unique=True)
    source_date = CharField()
    content = TextField()
    harvest_date = DateTimeField(default=datetime.now)

    class Meta:
        database = DATABASE


def create_tables():
    with DATABASE:
        DATABASE.create_tables([Item])


def get_start_date(config):
    with DATABASE:
        return (
            Item.select(fn.Max(Item.source_date)).where(
                Item.harvester == config["harvester_name"]
            )
        ).scalar() or config["start_date"]


def harvest_products(config, url):
    """Harvest the products."""
    while url:
        start_time = time()

        entries, url = harvest(url)

        items = (convert(Item, entry) for entry in entries)
        save_items(items)
        logger.debug("Saved items")

        if url:
            pause(start_time)
        else:
            continue

    logger.debug("Harvest complete")


def harvest(url):
    raw_feed = get_feed(url)
    parsed_feed = parse_feed(raw_feed)
    entries = get_entries(parsed_feed)
    url = add_orderby_if_missing(get_next_link(parsed_feed))

    return (entries, url)


@retry(wait=wait_fixed(60), stop=stop_after_attempt(3))
def get_feed(url):
    """Get the feed and raise an exception if the response is not 200."""
    logger.debug("Fetching...")
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()

    return r.text


def parse_feed(xml):
    """Parse the results feed."""
    return BeautifulSoup(xml, "lxml")


def get_entries(feed):
    """Get all the entries from the feed."""
    return feed.find_all("entry")


def convert(Item, entry):
    """Convert an OpenSearch entry into an item that can be saved in the db."""
    logger.debug(f"Found {get_identifier(entry)}")
    return Item(
        harvester=HARVESTER_NAME,
        source=SOURCE_NAME,
        identifier=get_identifier(entry),
        source_date=get_ingestiondate(entry),
        content=entry.encode(),
    )


def get_identifier(entry):
    """Get the product's identifier, i.e., the id used by the community."""
    return entry.find("str", {"name": "identifier"}).text


def get_ingestiondate(entry):
    """
    Get the product's ingestiondate
    (the date that the product was added to the source db).
    """
    return entry.find("date", {"name": "ingestiondate"}).text


def get_next_link(feed):
    """Get the next link. Return None if there's no next link."""
    next_link = feed.find("link", {"rel": "next", "type": "application/atom+xml"})

    if next_link:
        return next_link["href"]
    else:
        return None


def add_orderby_if_missing(next_link):
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


def pause(start_time):
    """Pause for up to one second to prevent the source from being overwhelmed."""
    time_elapsed = time() - start_time

    if time_elapsed < 1:
        sleep(1 - time_elapsed)
    else:
        pass


def save_items(items):
    """Save all the unique items in the db."""
    for item in items:
        with DATABASE.atomic():
            try:
                save_item(item)
            except IntegrityError:
                pass


@retry(
    retry=retry_if_exception_type(InternalError)
    | retry_if_exception_type(OperationalError),
    wait=wait_fixed(2),
    stop=stop_after_attempt(3),
)
def save_item(item):
    """Save the item int the db."""
    item.save()


def get_start_url(url_template, start_date):
    logger.debug(f"Start date: {start_date}")
    return url_template.format(start_date=start_date)


def get_config(config_filepath):
    return toml(str(Path(config_filepath)))


def initialize_database(database_filepath):
    DATABASE.init(str(Path(database_filepath)))
    with DATABASE:
        DATABASE.create_tables([Item])


@pidfile(HARVESTER_NAME, piddir=appdir)
def run(config_file):
    config = get_config(config_file)
    initialize_database(config["database"])
    logger.debug(
        f"Harvesting from {{config['source_name']}} using {{config['harvester_name']}}..."  # noqa: E501
    )
    start_date = get_start_date(config)
    start_url = get_start_url(config["url_template"], start_date)
    harvest_products(config, start_url)


@click.command()
@click.argument("config_file", type=str)
def cli(config_file):
    run(config_file)
