from datetime import datetime
from pathlib import Path
from time import time, sleep

from appdirs import user_data_dir
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
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from toml import load as toml


APPDIR = user_data_dir("Harvester")
DATABASE = SqliteDatabase(None)


class Item(Model):
    harvester = CharField()
    source = CharField()
    identifier = TextField(unique=True)
    source_date = CharField()
    content = TextField(null=True)
    harvest_date = DateTimeField(default=datetime.now)

    class Meta:
        database = DATABASE


def get_config(config_filepath):
    return toml(str(Path(config_filepath)))


class BaseHarvester(object):
    def __init__(self, config):
        self.name = config["harvester_name"]
        self.source = config["source_name"]
        self.database = config["database"]
        self.url_template = config["url_template"]
        self.start_date = config["start_date"]
        self.config = config

    def run(self):
        self._initialize_database()
        logger.debug(
            f"Harvesting from {{config['source_name']}} using {{config['harvester_name']}}..."  # noqa: E501
        )
        self.start_date = self._get_start_date()
        start_url = self._get_start_url(self.start_date)
        self._harvest_products(start_url)

    def _initialize_database(self):
        DATABASE.init(str(Path(self.database)))
        with DATABASE:
            DATABASE.create_tables([Item])

    def _get_start_date(self):
        with DATABASE:
            return (
                Item.select(fn.Max(Item.source_date)).where(Item.harvester == self.name)
            ).scalar() or self.start_date

    def _get_start_url(self, start_date):
        raise Exception("_get_start_url not implemented.")

    def _harvest_products(self, url):
        """Harvest the products."""
        while url:
            start_time = time()

            entries, url = self._harvest(url)
            logger.debug(entries)
            items = (self._convert(entry) for entry in entries)
            logger.debug(f"items={items}")
            self._save_items(items)
            logger.debug("Saved items")

            if url:
                self._pause(start_time)
            else:
                continue

        logger.debug("Harvest complete")

    def _harvest(self, url):
        raise Exception("_harvest not implemented.")

    def _convert(self, entry):
        raise Exception("_convert not implemented.")

    def _save_items(self, items):
        """Save all the unique items in the db."""
        for item in items:
            with DATABASE.atomic():
                try:
                    self._save_item(item)
                    logger.debug(f"saved {item}")
                except IntegrityError as e:
                    logger.debug(e)
                    pass

    @retry(
        retry=retry_if_exception_type(InternalError)
        | retry_if_exception_type(OperationalError),
        wait=wait_fixed(2),
        stop=stop_after_attempt(3),
    )
    def _save_item(self, item):
        """Save the item int the db."""
        item.save()

    def _pause(self, start_time):
        """Pause for up to one second to prevent the source from being overwhelmed."""
        time_elapsed = time() - start_time

        if time_elapsed < 1:
            sleep(1 - time_elapsed)
        else:
            pass


def main(config_file):
    config = get_config(config_file)
    # from dhus_harvester import DHuSHarvester as Harvester
    Harvester = getattr(
        __import__(config["harvester_module"]), config["harvester_class"]
    )
    harvester = Harvester(config)

    @pidfile(harvester.name, piddir=APPDIR)
    def run(harvester):
        harvester.run()

    run(harvester)


@click.command()
@click.argument("config_file", type=str)
def cli(config_file):
    main(config_file)
