"""Client for transforming workspaces."""

import logging

from anvil.terra.blob import Blob


class Transformer(object):
    """Render workspace into target form, Subclass and override transform_* per your requirements."""

    def __init__(self, *args, workspace):
        """Initialize instance."""
        self._logger = logging.getLogger(__name__)
        self.workspace = workspace

    def transform(self):
        """Transform entities."""
        for w in self.transform_workspace(self.workspace):
            yield w
        for subject in self.workspace.subjects:
            for s in self.transform_subject(subject):
                yield s
                for sample in subject.samples:
                    for s in self.transform_sample(sample):
                        yield s
                        for blob in sample.blobs.values():
                            for b in self.transform_blob(Blob(blob, sample)):
                                yield b

    def transform_workspace(self, workspace):
        """Transform workspace (noop)."""
        yield workspace

    def transform_subject(self, subject):
        """Transform subject (noop)."""
        yield subject

    def transform_sample(self, sample):
        """Transform sample (noop)."""
        yield sample

    def transform_blob(self, blob):
        """Transform blob (noop)."""
        yield blob
