import logging
import asyncio
import time
import abc
from pathlib import Path
import gzip

from streamz import Stream
import attr

logger = logging.getLogger(__name__)

LIBRARY_NAME = 'numismatic'


@attr.s
class Exchange(abc.ABC):
    '''Base class for Exchanges'''
    # TODO: Write to a separate stream
    output_stream = attr.ib()
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
