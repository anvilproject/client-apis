import requests_cache

_CACHE_NAME = 'anvil_cache'


def install_cache():
    """Turns on cache."""
    requests_cache.install_cache(_CACHE_NAME)


def uninstall_cache():
    """Turns off cache."""
    requests_cache.core.uninstall_cache()


# turns on cache for all requests by default
# install_cache()
