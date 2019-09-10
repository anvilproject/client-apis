import requests_cache


def enable_cache():
    """Turns on cache."""
    requests_cache.install_cache('anvil_cache')


def disable_cache():
    """Turns off cache."""
    requests_cache.core.uninstall_cache()


# turns on cache for all requests by default
enable_cache()
