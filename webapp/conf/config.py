from heapq import merge

from . import config_default

configs = config_default.config

try:
    import config_override
    configs = merge(configs, config_override.configs)
except ImportError:
    pass