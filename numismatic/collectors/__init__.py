from .base import Collector
from .file import FileCollector



__all__ = ["Collector", "FileCollector"]


from ..libs.utils import make_subclass_factory

setattr(Collector, 'factory', make_subclass_factory('Collector'))
