import logging
import sys
from collections import defaultdict


class LogCapture(logging.Logger):
    """Capture warnings and errors in `entries`, assumes tuple is logged, entries.key = tuple[0]."""
    def __init__(self, name='anvil-etl_old-transformers'):
        self.entries = defaultdict(list)
        super(LogCapture, self).__init__(name=name)

    def _persist(self, msg):
        self.entries[msg[0]].append(msg)

    def error(self, msg, *args, **kwargs):
        super().error(msg, *args, **kwargs)
        self._persist(msg)

    def warning(self, msg, *args, **kwargs):
        super().warning(msg, *args, **kwargs)
        self._persist(msg)

    def clear(self):
        self.entries = defaultdict(list)


logger = LogCapture()
if len(logging.root.handlers) == 0:
    print("Please initialize logging.",  file=sys.stderr)
else:
    logger.addHandler(logging.root.handlers[0])


def _recursive_default_dict():
    """Recursive default dict, any key defaults to a dict."""
    return defaultdict(_recursive_default_dict)
