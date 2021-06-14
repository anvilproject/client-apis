"""Client for reconciling terra(google) blob."""

from attrdict import AttrDict


class Blob(object):
    """Represent terra(google) blob."""

    def __init__(self, blob, sample):
        """Simplify blob."""
        self.attributes = AttrDict(blob)
        self.sample = sample
