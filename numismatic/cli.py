import logging
import click
from itertools import chain, product
from collections import namedtuple
import asyncio
from streamz import Stream
import attr


logger = logging.getLogger(__name__)


DEFAULT_ASSETS = ['BTC']
DEFAULT_CURRENCIES = ['USD']
ENVVAR_PREFIX = 'NUMISMATIC'

pass_state = click.make_pass_decorator(dict, ensure=True)

@click.group(chain=True)
@click.option('--collector', '-c', default='CryptoCompare',
              type=click.Choice(['CryptoCompare', 'BFXData']))
@click.option('--cache-dir', '-d', default=None)
@click.option('--requester', '-r', default='caching',
              type=click.Choice(['base', 'caching']))
@click.option('--log-level', '-l', default='info', 
              type=click.Choice(['debug', 'info', 'warning', 'error',
                                 'critical']))
@pass_state
def numisma(state, collector, cache_dir, requester, log_level):
    '''Numismatic Command Line Interface

    Examples:

        coin list

        coin info

        coin info -a ETH

        coin prices

        coin prices -a XMR,ZEC -c EUR,ZAR

        NUMISMATIC_CURRENCIES=ZAR coin prices

        coin history

        coin history -a ETH -c BTC -o ETH-BTH-history.json

        coin listen collect run

        coin listen -a BTC,ETH,XMR,ZEC collect -t Trade run -t 30
    '''
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    from .collectors import CoinCollector
    state['collector'] = \
        CoinCollector.factory(collector_name=collector, cache_dir=cache_dir,
                              requester=requester)
    state['output_stream'] = Stream()
    state['subscriptions'] = {}

@numisma.command(name='list')
@click.option('--output', '-o', type=click.File('wt'), default='-')
@pass_state
def list_all(state, output):
    "List all available assets"
    collector = state['collector']
    assets_list = collector.get_list().keys()
    write(assets_list, output, sep=' ')

@numisma.command()
@click.option('--assets', '-a', multiple=True, default=DEFAULT_ASSETS,
              envvar=f'{ENVVAR_PREFIX}_ASSETS')
@click.option('--output', '-o', type=click.File('wt'), default='-')
@pass_state
def info(state, assets, output):
    "Info about the requested assets"
    assets = ','.join(assets).split(',')
    collector = state['collector']
    all_info = collector.get_list()
    assets_info = [all_info[a] for a in assets]
    write(assets_info, output)


@numisma.command()
@click.option('--assets', '-a', multiple=True, default=DEFAULT_ASSETS,
              envvar=f'{ENVVAR_PREFIX}_ASSETS')
@click.option('--currencies', '-c', multiple=True, default=DEFAULT_CURRENCIES,
              envvar=f'{ENVVAR_PREFIX}_CURRENCIES')
@click.option('--output', '-o', type=click.File('wt'), default='-')
@pass_state
def prices(state, assets, currencies, output):
    'Latest asset prices'
    assets = ','.join(assets)
    currencies = ','.join(currencies)
    collector = state['collector']
    prices = collector.get_prices(coins=assets, currencies=currencies)
    write(prices, output)


@numisma.command()
@click.option('--freq', '-f', default='d', type=click.Choice(list('dhms')))
@click.option('--start-date', '-s', default=-30)
@click.option('--end-date', '-e', default=None)
@click.option('--assets', '-a', multiple=True, default=DEFAULT_ASSETS,
              envvar=f'{ENVVAR_PREFIX}_ASSETS')
@click.option('--currencies', '-c', multiple=True, default=DEFAULT_CURRENCIES,
              envvar=f'{ENVVAR_PREFIX}_CURRENCIES')
@click.option('--output', '-o', type=click.File('wt'), default='-')
@pass_state
def history(state, assets, currencies, freq, start_date, end_date, output):
    'Historic asset prices and volumes'
    assets = ','.join(assets).split(',')
    currencies = ','.join(currencies).split(',')
    collector = state['collector']
    data = []
    for asset, currency in product(assets, currencies):
        pair_data = collector.get_historical_data(
            asset, currency, freq=freq, start_date=start_date, end_date=end_date)
        data.extend(pair_data)
    write(data, output)


def tabulate(data):
    if isinstance(data, dict):
        data_iter = data.values()
    elif isinstance(data, list):
        data_iter = data
    else:
        raise TypeError(f'data: {data}')
    headers = set(chain(*map(lambda v: v.keys(), data_iter)))
    DataTuple = namedtuple('DataTuple', ' '.join(headers))
    DataTuple.__new__.__defaults__ = (None,) * len(headers)
    return map(lambda d: DataTuple(**d), data_iter)


@numisma.command()
@click.option('--exchange', '-e', default='bitfinex',
              type=click.Choice(['bitfinex', 'bitstamp', 'gdax', 'gemini',
                                 'poloniex']))
@click.option('--assets', '-a', multiple=True, default=DEFAULT_ASSETS,
              envvar=f'{ENVVAR_PREFIX}_ASSETS')
@click.option('--currencies', '-c', multiple=True, default=DEFAULT_CURRENCIES,
              envvar=f'{ENVVAR_PREFIX}_CURRENCIES')
@click.option('--channel', '-C', default='trades', type=click.Choice(['trades', 'ticker']))
@pass_state
def listen(state, exchange, assets, currencies, channel):
    'Listen to live events from an exchange'
    from .exchanges import BitfinexExchange
    output_stream = state['output_stream']
    subscriptions = state['subscriptions']
    if exchange=='bitfinex':
        exchange = BitfinexExchange(output_stream=output_stream)
        assets = ','.join(assets).split(',')
        currencies = ','.join(currencies).split(',')
        pairs = list(map(''.join, product(assets, currencies)))
        for pair in pairs:
            subscription = exchange.listen(pair, channel)
            subscriptions[f'{pair}({exchange})'] = subscription
    else:
       raise NotImplementedError()
 

@numisma.command()
@click.option('--filter', '-f', default='', type=str, multiple=True)
@click.option('--type', '-t', default=None,
              type=click.Choice([None, 'Trade', 'Heartbeat']))
@click.option('--output', '-o', default='-', type=click.File('w'))
@click.option('--events', 'format', flag_value='events', default=True)
@click.option('--json', 'format', flag_value='json')
@pass_state
def collect(state, filter, type, output, format):
    'Collect events and write them to an output sink'
    output_stream = state['output_stream']
    if type:
        output_stream = output_stream.filter(
            lambda ev: ev.__class__.__name__==type)
    filters = filter
    for filter in filters:
        output_stream = output_stream.filter(
            lambda x: eval(filter, attr.asdict(x)))
    if format=='json':
        output_stream = output_stream.map(attr.asdict)
    sink = (output_stream
            .map(lambda ev: output.write(str(ev)+'\n'))
            .map(lambda ev: output.flush())
            )


@numisma.command()
@click.option('--timeout', '-t', default=15)
@pass_state
def run(state, timeout):
    'Run the asyncio event loop for a set amount of time'
    subscriptions = state['subscriptions']
    loop = asyncio.get_event_loop()
    logger.debug('starting ...')
    completed, pending = \
        loop.run_until_complete(asyncio.wait(subscriptions.values(),
                                timeout=timeout))
    logger.debug('cancelling ...')
    for task in pending:
        task.cancel()
    logger.debug('finishing...')
    loop.run_until_complete(asyncio.sleep(1))
    logger.debug('done')


def write(data, file, sep='\n'):
    for record in data:
        file.write(str(record)+sep)


if __name__ == '__main__':
    numisma(auto_envvars_prefix=f'{ENVVAR_PREFIX}')
