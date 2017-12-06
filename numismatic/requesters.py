import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from pathlib import Path
from functools import partial
import gzip
from urllib.parse import urlparse, urlencode
import pickle

import attr
from appdirs import user_cache_dir
from .config import config_item_getter


log = logging.getLogger(__name__)


LIBRARY_NAME = 'numismatic'


@attr.s
class Requester:
    "Basic Requester using requests and blocking calls"

    retries = attr.ib(default=attr.Factory(
        config_item_getter('REQUESTER', 'retries')),
        convert=int)
    timeout = attr.ib(default=attr.Factory(
        config_item_getter('REQUESTER', 'timeout')),
        convert=int)
    backoff_factor = attr.ib(default=attr.Factory(
        config_item_getter('REQUESTER', 'backoff_factor')),
        convert=float)
    status_forcelist = attr.ib(default=attr.Factory(
        config_item_getter('REQUESTER', 'status_forcelist')),
        convert=lambda val: tuple(map(int, val.split(','))),
        validator=attr.validators.instance_of(tuple))

    @classmethod
    def factory(cls, requester, **kwargs):
        requester = '' if requester is None else requester.lower()
        # FIXME: use the subclass method used in Feed and Collector
        if requester in {'', 'base', 'basic'}:
            subcls = Requester
        elif requester in {'caching'}:
            subcls = CachingRequester
        else:
            raise NotImplementedError(f'requester={requester}')
        kwds = {field.name:kwargs[field.name] for field in
                attr.fields(subcls) if field.name in kwargs}
        return subcls(**kwds)

    def get(self, url, params=None, headers=None):
        session = requests.Session()
        retry = Retry(
            total=self.retries,
            read=self.retries,
            connect=self.retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        response = session.get(url, params=params, headers=headers, timeout=self.timeout)
        return response


@attr.s
class CachingRequester(Requester):

    cache_dir = \
        attr.ib(default=attr.Factory(partial(user_cache_dir, LIBRARY_NAME)),
                convert=Path)

    def _get_path(self, url, params=None):
        parts = urlparse(url)
        path = self.cache_dir / parts.netloc / parts.path.strip('/')
        if params:
            path /= urlencode(params)
        return path

    def get(self, url, params=None, headers=None, use_cache=True):
        path = self._get_path(url, params=params, headers=headers)
        if use_cache and path.exists():
            try:
                return pickle.load(path.open('rb'))
            except:
                # Couldn't load cached response so make request
                log.error(f'Couldn\'t load {path}.')
        log.debug(f'Retrieving {url} ...')
        response = super().get(url, params=params)
        # FIXME: should this raise if unsuccessful?
        if not path.parent.exists():
            log.debug(f'Creating {path.parent} ...')
            path.parent.mkdir(parents=True)
        log.debug(f'Saving {path} ...')
        pickle.dump(response, path.open('wb'))
        return response
        # if str(path).endswith('.gz'):
        #     opener = partial(gzip.open, mode='wb')
        # else:
        #     opener = partial(open, mode='wt')
        # path = request_dir / filename
        # log.debug(f'Saving {path} ...')
        # with open(path) as f:
        #     for chunk in response.iter_content(chunk_size=1024):
        #         f.write(chunk)
        # return response


class AsyncRequester(Requester):
    "Asynchronous requester with rate limiting"

    def get(self, url, path=None):
        raise NotImplementedError()

