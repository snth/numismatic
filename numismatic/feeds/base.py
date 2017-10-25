import logging
import time
import abc
from pathlib import Path
import gzip

from streamz import Stream
import attr

from ..libs.requesters import Requester

logger = logging.getLogger(__name__)


LIBRARY_NAME = 'numismatic'


@attr.s
class Feed(abc.ABC):
    "Feed Base class"

    rest_api = attr.ib(default=None)
    websocket_client = attr.ib(default=None)
        
    @abc.abstractmethod
    def get_list(self):
        return

    @abc.abstractmethod
    def get_info(self, assets):
        return

    @abc.abstractmethod
    def get_prices(self, assets, currencies):
        return

    def __getattr__(self, attr):
        if self.rest_api is not None and hasattr(self.rest_api, attr):
            return getattr(self.rest_api, attr)
        elif self.websocket_client is not None and hasattr(self.ws_api, attr):
            return getattr(self.websocket_client, attr)
        else:
            raise AttributeError


@attr.s
class RestApi(abc.ABC):

    cache_dir = attr.ib(default=None)
    requester = attr.ib(default='base')

    @requester.validator
    def __requester_validator(self, attribute, value):
        if isinstance(value, str):
            requester = Requester.factory(value, cache_dir=self.cache_dir)
            setattr(self, attribute.name, requester)
        elif not isinstance(value, Requester):
            raise ValueError(f'{attribute.name}: {value}')

    def _make_request(self, api_url, params=None, headers=None):
        response = self.requester.get(api_url, params=params, headers=headers)
        data = response.json()
        return data


@attr.s
class WebsocketApi(abc.ABC):
    '''Base class for WebsocketApi feeds'''
    # TODO: Write to a separate stream
    output_stream = attr.ib(default=None)
    raw_stream = attr.ib(default=None)
    raw_interval = attr.ib(default=1)

    @abc.abstractmethod
    async def listen(self, symbol):
        if self.raw_stream is not None:
            # FIXME: Use a FileCollector here
            if self.raw_stream=='':
                from appdirs import user_cache_dir
                self.raw_stream = user_cache_dir(LIBRARY_NAME)
            date = time.strftime('%Y%m%dT%H%M%S')
            filename = f'{self.exchange}_{symbol}_{date}.json.gz'
            raw_stream_path = str(Path(self.raw_stream) / filename)
            logger.info(f'Writing raw stream to {raw_stream_path} ...')

            def write_to_file(batch):
                logger.debug(f'Writing batch of {len(batch)} for {symbol} ...')
                with gzip.open(raw_stream_path, 'at') as f:
                    for packet in batch:
                        f.write(packet+'\n')

            self.raw_stream = Stream()
            (self.raw_stream
             .timed_window(self.raw_interval)
             .filter(len)
             .sink(write_to_file)
             )
             

    @abc.abstractmethod
    def _handle_packet(self, packet, symbol):
        # record the raw packets on the raw_stream
        if self.raw_stream is not None:
            self.raw_stream.emit(packet)
