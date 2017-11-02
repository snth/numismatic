import logging
import requests
import time
from pathlib import Path
from functools import partial
import gzip
from urllib.parse import urlparse, urlencode
import pickle

import attr
from appdirs import user_cache_dir


log = logging.getLogger(__name__)


LIBRARY_NAME = 'numismatic'


@attr.s
class Requester:
    "Basic Requester using requests and blocking calls"

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
        response = requests.get(url, params=params, headers=headers)
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

