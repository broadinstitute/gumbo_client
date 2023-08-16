import importlib.metadata

__version__ = importlib.metadata.version('gumbo-client')

from .client import Client
