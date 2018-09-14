from __future__ import print_function

import sys



from datetime import datetime
from pathlib import Path
from time import time, sleep

from ckan.lib.cli import load_config

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

from harvest import DATABASE, Item

APPDIR = user_data_dir("Loader")


class LastLoadedItem(Model):
    loader = CharField()
    harvester = CharField()
    harvest_date = DateTimeField(default=datetime(1900, 1, 1))

    class Meta:
        database = DATABASE


def get_config(config_filepath):
    return toml(str(Path(config_filepath)))


class BaseLoader(object):
    def __init__(self, config):
        self.name = config["loader_name"]
        self.harvester = config["harvester_name"]
        self.database = config["database"]
        self.config = config

    def run(self):
        self._initialize_database()
        self.last_loaded = self._get_last_loaded()
        logger.debug(
            "Loading from {} using {}...".format(self.harvester, self.name)
        )
        self._load_products()

    def _initialize_database(self):
        DATABASE.init(str(Path(self.database)))
        with DATABASE:
            DATABASE.create_tables([LastLoadedItem])

    def _load_products(self):
        """Load the products."""

        products_to_load = self._get_products_to_load()

        if products_to_load:
            for product in products_to_load:
                logger.debug(product.identifier)
            self._prepare()

        while products_to_load:
            data_dicts = (self._transform(product) for product in products_to_load)
            for data_dict, harvest_date in data_dicts:
                logger.debug(data_dict)
                self._load(data_dict)
                self._update_last_loaded(harvest_date)
            products_to_load = self._get_products_to_load()

        logger.debug("Loading complete")

    def _get_products_to_load(self):
        with DATABASE:
            return Item.select().where((Item.harvester == self.harvester) & (Item.harvest_date > self.last_loaded.harvest_date)).order_by(Item.harvest_date.asc()).limit(100) or None

    def _get_last_loaded(self):
        with DATABASE:
            last_loaded, status = LastLoadedItem.get_or_create(loader=self.name, harvester=self.harvester)
            logger.debug(last_loaded.harvest_date)
            return last_loaded

    def _update_last_loaded(self, harvest_date):
        with DATABASE:
            self.last_loaded.harvest_date = harvest_date
            self.last_loaded.save()

    def _transform(self, harvest_item):
        print(harvest_item.identifier)
        return (
            {
                "owner_org": "test_org",
                "name": harvest_item.identifier.lower()
            },
            harvest_item.harvest_date
        )

    def _prepare(self):
        logger.debug("creting context")
        self.context = self._create_ckan_context()

        logger.debug("created context")

    def _load(self, data_dict):
        try:
            self.get_action("package_create")(self.context, data_dict)
        except self.ValidationError as e:
            logger.debug(e)
            pass

    def _create_ckan_context(self):
        # load_site_user=True
        site_user = load_config(self.config["ckan_config"], True)

        from ckan import model
        self.model = model

        from ckan.logic import get_action
        self.get_action = get_action
        from ckan.logic import ValidationError
        self.ValidationError = ValidationError

        return {
            "model": self.model,
            "session": self.model.Session,
            "user": site_user["name"],
            "ignore_auth": True,
            "return_id_only": True
        }


def main(config_file):
    config = get_config(config_file)
    # from dhus_harvester import DHuSHarvester as Harvester
    # Loader = getattr(
    #     __import__(config["loader_module"]), config["loader_class"]
    # )
    loader = BaseLoader(config)

    @pidfile(loader.name, piddir=APPDIR)
    def run(loader):
        loader.run()

    run(loader)

@click.command()
@click.argument("config_file", type=str)
def cli(config_file):
    main(config_file)