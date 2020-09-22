"""Client for reconciling terra subject."""

import logging
from attrdict import AttrDict


class Subject(object):
    """Represent terra subject."""

    def __init__(self, *args, workspace=None, samples=None):
        """Pass all args to AttrDict."""
        self.attributes = AttrDict(*args)
        self._logger = logging.getLogger(__name__)
        self.workspace_name = workspace.name
        self.schema = workspace.subject_schema
        self.sample_schema = workspace.sample_schema
        self.subject_property_name = workspace.subject_property_name
        self.namespace = workspace.attributes.workspace.namespace
        # find all samples associated with blobs
        self.samples = self._find_samples(samples)

    def __repr__(self):
        """Return attributes."""
        return str(self.attributes)

    @property
    def id(self):
        """Delegate to subclass."""
        raise Exception('Not implemented')

    def _find_samples(self, samples):
        """Get samples."""
        return samples[self.id]

    @property
    def missing_samples(self):
        """Test if no samples."""
        return len(self.samples) == 0


class CCDGSubject(Subject):
    """Extend Subject class."""

    def __init__(self, *args, workspace=None, samples=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, samples=samples)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name


class CMGSubject(Subject):
    """Extend Subject class."""

    def __init__(self, *args, workspace=None, samples=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, samples=samples)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name


class GTExSubject(Subject):
    """Extend Subject class."""

    def __init__(self, *args, workspace=None, samples=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, samples=samples)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name


class ThousandGenomesSubject(Subject):
    """Extend Subject class."""

    def __init__(self, *args, workspace=None, samples=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, samples=samples)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name


class eMERGESUbject(Subject):
    """Extend Subject class."""

    def __init__(self, *args, workspace=None, samples=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, samples=samples)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name


def subject_factory(*args, **kwargs):
    """Return a specialized Subject class instance."""
    if 'CCDG' in kwargs['workspace'].name.upper():
        return CCDGSubject(*args, **kwargs)
    if 'CMG' in kwargs['workspace'].name.upper():
        return CMGSubject(*args, **kwargs)
    if 'GTEX' in kwargs['workspace'].name.upper():
        return GTExSubject(*args, **kwargs)
    if '1000G-HIGH-COVERAGE' in kwargs['workspace'].name.upper():
        return ThousandGenomesSubject(*args, **kwargs)
    if 'ANVIL_EMERGE' in kwargs['workspace'].name.upper():
        return eMERGESUbject(*args, **kwargs)
    raise Exception('Not implemented')
