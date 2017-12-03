import logging
import sys
from functools import partial

import attr

from .base import Collector
from ..events import OrderType, Trade, Order

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, \
        Float, String
    TYPE_MAPPING = {int:Integer, float:Float, str:String, OrderType:String}
except ImportError:
    MISSING_IMPORT_WARNING = (
        'You need to have sqlalchemy installed to use the '
        'SqlCollector. Install it with:\n'
        '\n'
        '   pip install sqlalchemy'
        '\n')


@attr.s
class SqlCollector(Collector):

    path = attr.ib(default='sqlite:///:memory:')
    format = attr.ib(default='json')
    interval = attr.ib(default=None)

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        if 'MISSING_IMPORT_WARNING' in globals():
            logger.error(MISSING_IMPORT_WARNING)
            sys.exit(1)
        engine = create_engine(self.path, echo=False)

        metadata = MetaData()

        self._store_events_of_type(Trade, engine, metadata)
        self._store_events_of_type(Order, engine, metadata)

    @staticmethod
    def _make_table_from_attrs(attrs_cls, table_name=None, metadata=None):
        metadata = MetaData() if metadata is None else metadata
        columns = [Column(attribute.name, TYPE_MAPPING[attribute.convert]) 
                   for attribute in attr.fields(attrs_cls)]
        table_name = table_name if table_name else (
            attrs_cls.__name__.lower() + 's')
        table_obj = Table(table_name, metadata, *columns)
        return table_obj

    def _store_events_of_type(self, event_type, engine, metadata):
        # filter events of the type
        event_type_stream = \
            self.event_stream.filter(lambda ev: isinstance(ev, event_type))

        # construct data_stream
        json_stream = event_type_stream.map(attr.asdict)

        if self.interval:
            data_stream = json_stream.timed_window(interval=self.interval)
        else:
            # ensure downstream receives lists rather than elements
            data_stream = json_stream.partition(1)

        # create the necessary tables
        events_table = \
            self._make_table_from_attrs(event_type, metadata=metadata)
        metadata.create_all(engine)

        # prepare the insert statement
        events_table_insert = events_table.insert()
        # trades_insert.bind = engine

        # sink to the database
        conn = engine.connect()
        data_stream.filter(len).sink(
            partial(conn.execute, events_table_insert))

