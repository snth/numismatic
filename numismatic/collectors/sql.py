import logging
import sys
from functools import partial

import attr

from .base import Collector
from ..events import Trade

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
except ImportError:
    logger.error('You need to have sqlalchemy installed to use the '
                'SqlCollector. Install it with:\n'
                '\n'
                '   pip install sqlalchemy'
                '\n')
    sys.exit(1)

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, \
    Float, String
TYPE_MAPPING = {int:Integer, float:Float, str:String}


@attr.s
class SqlCollector(Collector):

    path = attr.ib(default='sqlite:///:memory:')
    format = attr.ib(default='json')
    interval = attr.ib(default=None)

    def __attrs_post_init__(self):
        engine = create_engine(self.path, echo=False)

        metadata = MetaData()
        columns = [Column(attribute.name, TYPE_MAPPING[attribute.convert]) 
                   for attribute in attr.fields(Trade)]
        trades = Table('trades', metadata, *columns)

        # create tables where required
        metadata.create_all(engine)

        # prepare the insert statement
        trades_insert = trades.insert()
        # trades_insert.bind = engine

        # construct data_stream
        self._json_stream = self.event_stream.map(attr.asdict)
        if self.interval:
            self._data_stream = \
                self._json_stream.timed_window(interval=self.interval)
        else:
            # ensure downstream receives lists rather than elements
            self._data_stream = \
                self._json_stream.partition(1)

        # sink to the database
        conn = engine.connect()
        self._data_stream.filter(len).sink(
            partial(conn.execute, trades_insert))
