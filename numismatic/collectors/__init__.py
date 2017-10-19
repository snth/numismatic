from .base import Collector
from .file import FileCollector



__all__ = ["Collector", "FileCollector"]


from ..libs.utils import make_get_subclasses, subclass_factory

setattr(Collector, '_get_subclasses', make_get_subclasses('Collector'))
setattr(Collector, 'factory', subclass_factory)
