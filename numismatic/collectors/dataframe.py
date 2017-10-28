import logging
import sys
from functools import partial

import attr
from streamz.dataframe import StreamingDataFrame

from .base import Collector
from ..events import OrderType, Trade, Order

logger = logging.getLogger(__name__)

try:
    import pandas as pd
except ImportError:
    logger.error('You need to have pandas installed to use the '
                'DataFrameCollector. Install it with:\n'
                '\n'
                '   pip install pandas\n'
                'or\n'
                '   pip install numismatic[dataframe]\n')
    sys.exit(1)

TYPE_MAPPING = {int:int, float:float, str:str, OrderType:str}


@attr.s
class DataframeCollector(Collector):

    path = attr.ib(default='-')
    format = attr.ib(default='json')
    interval = attr.ib(default=None)

    def __attrs_post_init__(self):
        # paths
        if self.path=='-':
            self._opener = lambda: sys.stdout
        elif self.path.endswith('.gz'):
            self._opener = partial(gzip.open, self.path, mode='at')
        else:
            self._opener = partial(open, self.path, mode='at')

        self._store_events_of_type(Trade)
        self._store_events_of_type(Order)

    @staticmethod
    def _make_dataframe_from_attrs(attrs_cls, dataframe_name=None):
        columns = [attribute.name for attribute in attr.fields(attrs_cls)]
        data = {attribute.name:[] for attribute in attr.fields(attrs_cls)}
        dataframe_name = dataframe_name if dataframe_name else (
            attrs_cls.__name__.lower() + 's')
        df = pd.DataFrame(data, columns=columns)
        return df

    def _store_events_of_type(self, event_type):
        # create the necessary example
        events_dataframe = self._make_dataframe_from_attrs(event_type)

        # filter events of the type
        event_type_stream = \
            self.event_stream.filter(lambda ev: isinstance(ev, event_type))

        # construct data_stream
        dict_stream = event_type_stream.map(attr.asdict)

        if self.interval:
            data_stream = dict_stream.timed_window(interval=self.interval)
        else:
            # ensure downstream receives lists rather than elements
            data_stream = dict_stream.partition(1)

        # stream of DataFrames
        df_stream = data_stream.filter(len).map(
            partial(pd.DataFrame.from_records, 
                    columns=events_dataframe.columns)
        )

        # create a StreamingDataFrame
        sdf = StreamingDataFrame(df_stream, example=events_dataframe)

        sdf.stream.sink(self.write)


    def write(self, df):
        # TODO: Use aiofiles for non-blocking IO here
        file = self._opener()
        try:
            file.write(str(df)+'\n\n')
            file.flush()
        finally:
            if self.path!='-':
                file.close()
