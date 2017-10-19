from .base import Collector
from .file import FileCollector

__all__ = ["Collector", "FileCollector"]


def collector_factory(cls, collector_name, *args, **kwargs):
    if not isinstance(collector_name, str):
        raise TypeError(f'"collector_name" must be a str. '
                        'Not {type(collector_name)}.')
    collector_name = collector_name.lower()
    subclasses = {subcls.__name__.lower()[:-len('Collector')]:subcls 
                    for subcls in cls.__subclasses__()}
    subclass = subclasses[collector_name]
    collector = subclass(*args, **kwargs)
    return collector


setattr(Collector, 'factory', classmethod(collector_factory))
