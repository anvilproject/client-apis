"""Client for transforming workspaces."""

import logging


class Transformer(object):
    """Render workspace into target form, Subclass and override transform_* per your requirements."""

    def __init__(self, *args, workspace):
        """Initialize instance."""
        self._logger = logging.getLogger(__name__)
        self.workspace = workspace

    def transform(self):
        """Transform entities."""
        try:
            for w in self.transform_workspace(self.workspace):
                yield w
            for subject in self.workspace.subjects:
                for s in self.transform_subject(subject):
                    yield s
                    for sample in subject.samples:
                        for s in self.transform_sample(sample, subject):
                            yield s
        except Exception as e:
            logging.getLogger(__name__).warning(f"{self.workspace.id} {e}")

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
