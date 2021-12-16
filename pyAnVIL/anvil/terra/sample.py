"""Client for reconciling terra subject."""

import logging
from anvil.gen3.drs_files import DRSFiles
from attrdict import AttrDict
from collections import defaultdict
import os


# drs entries
drs_files = None

# controls logging and drs lookup
sample_exceptions = []
SKIP_DRS = "skip_drs"


def _shorten_workspace(name):
    name = name.replace('AnVIL_', '')
    name = name.replace('CMG_', '')
    name = name.replace('CCDG_', '')
    return name


def _append_drs(sample):
    """Add ga4gh_drs_uri to blob."""
    global sample_exceptions
    if SKIP_DRS in sample_exceptions:
        return
    try:
        for key in sample.blobs.keys():
            filename = key.split('/')[-1]
            drs_file = drs_files.find_by_file_name(filename)
            assert drs_file, "Should have a gen3 sequencing record."
            sample.blobs[key]['ga4gh_drs_uri'] = drs_file['ga4gh_drs_uri']
            sample.blobs[key]['md5sum'] = drs_file['md5sum']
            assert sample.blobs[key]['ga4gh_drs_uri'], "Should have a ga4gh_drs_uri"
    except Exception as e:
        if sample.workspace_name not in sample_exceptions:
            logging.warn(f"DRS not found {sample.workspace_name} {sample.id} (supressing further errors for this workspace) {e}")
            sample_exceptions.append(sample.workspace_name)


class Sample(object):
    """Represent terra sample."""

    @staticmethod
    def skip_drs():
        """Disable DRS expensive lookup."""
        global sample_exceptions
        sample_exceptions.append(SKIP_DRS)

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Pass all args to AttrDict."""
        global drs_files
        self._logger = logging.getLogger(__name__)
        self.attributes = AttrDict(*args)
        self.missing_blobs = True
        self.missing_sequence = False
        self.schema = workspace.sample_schema
        self.workspace_name = workspace.name
        self.missing_blob_path = False
        self.drs_file_path = drs_file_path
        self.blobs = self._find_blobs(blobs, sequencing)

        if not drs_files and drs_file_path:
            if os.path.isfile(drs_file_path):
                drs_files = DRSFiles(drs_file_path)
            else:
                if 'drs_file_path' not in sample_exceptions:
                    self._logger.warn(f"{drs_file_path} should exist. Please export anvil_extract drs-file")
                    sample_exceptions.append('drs_file_path')
        if drs_files:
            _append_drs(self)

    def _find_blobs(self, blobs, sequencing):
        """Find all blobs associated with sample."""
        self._logger.debug("_find_blobs")
        blob_names = [(property_name, blob_name) for property_name, blob_name in self.attributes.attributes.items() if isinstance(blob_name, str) and blob_name.startswith('gs://')]
        my_blobs = []
        for property_name, blob_name in blob_names:
            if 'md5' in blob_name:
                # print(f'skipping md5 {blob_name}')
                continue
            blob = blobs.get(blob_name, None)
            if blob:
                blob['property_name'] = property_name
            my_blobs.append(blob)
        self.missing_blobs = len([b for b in my_blobs if b]) == 0
        return {b['name']: b for b in my_blobs if b}

    def __repr__(self):
        """Return attributes."""
        return str(self.attributes)

    @property
    def blob_sizes(self):
        """Aggregate blob sizes by property name."""
        _blob_sizes = defaultdict(int)
        for b in self.blobs.values():
            _blob_sizes[b['property_name']] += b['size']
        return _blob_sizes

    @property
    def id(self):
        """Delegate to subclass."""
        raise Exception('Not implemented')

    @property
    def subject_id(self):
        """Delegate to subclass."""
        raise Exception('Not implemented')

    @property
    def inconsistent_entityName(self):
        """Delegate to subclass."""
        raise Exception('Not implemented')

    @property
    def misspelled_subject(self):
        """Delegate to subclass."""
        raise Exception('Not implemented')


class CCDGSample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    @property
    def id(self):
        """Deduce id."""
        if 'project' not in self.attributes.attributes:
            # return f"{_shorten_workspace(self.workspace_name)}/Sa/{self.attributes.name}"
            return self.attributes.name
        # format the gen3 uses
        if 'collaborator_sample_id' not in self.attributes.attributes:
            # return f"{self.attributes.attributes['project']}_{self.attributes.attributes['sample']}"
            return self.attributes.attributes['sample']
        # return f"{self.attributes.attributes['project']}_{self.attributes.attributes['collaborator_sample_id']}"
        return self.attributes.attributes['collaborator_sample_id']

    @property
    def subject_id(self):
        """Deduce id."""
        if self.inconsistent_entityName:
            if self.inconsistent_subject:
                # this project has no subjects?
                if self.workspace_name == 'AnVIL_CCDG_WashU_CVD_EOCAD_BioImage_WGS':
                    return None
                if 'participent' not in self.attributes.attributes:
                    return None
                return self.attributes.attributes.participent
            return self.attributes.attributes.participant
        return self.attributes.attributes.participant.entityName

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        if self.inconsistent_subject or 'entityName' not in self.attributes.attributes.participant:
            return True
        return False

    @property
    def inconsistent_subject(self):
        """Flag misspellings."""
        if 'participant' not in self.attributes.attributes:
            return True
        return False


class CMGSample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    def _find_blobs(self, blobs, sequencing):
        """Find all blobs associated with sample."""
        blob_names = [(property_name, blob_name) for property_name, blob_name in self.attributes.attributes.items() if isinstance(blob_name, str) and blob_name.startswith('gs://')]
        if sequencing:
            try:
                sequence = [s for s in sequencing if 'collaborator_sample_id' in s['attributes'] and s['attributes']['collaborator_sample_id'] == self.id]
            except Exception as e:
                print(f'collaborator_sample_id not present {e}')
                print(self.id)
                print(sequencing[0]['attributes'].keys())
                print(sequencing[0])
                sequence = []

            if len(sequence) == 0:
                try:
                    sequence = [s for s in sequencing if 'sample_alias' in s['attributes'] and s['attributes']['sample_alias'] == self.id]
                except Exception:
                    print('sample_alias not present')
                    print(self.id)
                    print(sequencing[0]['attributes'].keys())
                    print(sequencing[0])
                    sequence = []

            if len(sequence):
                sequence = sequence[0]['attributes']
                blob_names.extend([(property_name, blob_name) for property_name, blob_name in sequence.items() if isinstance(blob_name, str) and blob_name.startswith('gs://')])
            else:
                self.missing_sequence = True

        # assert len(blob_names) > 0, self.workspace_name
        my_blobs = []
        for property_name, blob_name in blob_names:
            blob = blobs.get(blob_name, None)
            if blob:
                blob['property_name'] = property_name
            my_blobs.append(blob)
        # assert len(my_blobs) > 0, self.workspace_name
        self.missing_blobs = not len([b for b in my_blobs if not b]) == 0
        return AttrDict({b['name']: b for b in my_blobs if b})

    @property
    def subject_id(self):
        """Deduce id."""
        if self.inconsistent_subject:
            if 'participant' in self.attributes.attributes:
                return self.attributes.attributes.participant.entityName
            if 'subject_id' in self.attributes.attributes:
                return self.attributes.attributes.subject_id
            if 'participant_id' in self.attributes.attributes:
                return self.attributes.attributes.participant_id
            print(self.attributes)
            print(self.attributes.keys())
            assert False, 'inconsistent_subject unknown subject identifier'
        if '01-subject_id' not in self.attributes.attributes:
            print(self.attributes)
            print(self.attributes.keys())
        return self.attributes.attributes['01-subject_id']

    @property
    def inconsistent_subject(self):
        """Flag subject id inconsistencies."""
        if '01-subject_id' in self.attributes.attributes:
            return False
        return True

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        return False

    @property
    def id(self):
        """Deduce id."""
        # return f"{_shorten_workspace(self.workspace_name)}/Sa/{self.attributes.name}"
        return self.attributes['name']


class GTExSample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    @property
    def id(self):
        """Deduce id."""
        # return f"{_shorten_workspace(self.workspace_name)}/Sa/{self.attributes.name}"
        return self.attributes.name

    @property
    def subject_id(self):
        """Deduce id."""
        return self.attributes.attributes.participant.entityName

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        return False

    @property
    def misspelled_subject(self):
        """Flag misspellings."""
        return False

    @property
    def inconsistent_subject(self):
        """Flag misspellings."""
        return False


class ThousandGenomesSample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    @property
    def id(self):
        """Deduce id."""
        # return f"{self.workspace_name}/Sa/{self.attributes.name}"
        return self.attributes.name

    @property
    def subject_id(self):
        """Deduce id."""
        return self.attributes.attributes.participant

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        return False

    @property
    def misspelled_subject(self):
        """Flag misspellings."""
        return False

    @property
    def inconsistent_subject(self):
        """Flag misspellings."""
        return False


class eMERGESample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name

    @property
    def subject_id(self):
        """Deduce id."""
        return self.attributes.attributes.participant.entityName

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        return False

    @property
    def misspelled_subject(self):
        """Flag misspellings."""
        return False

    @property
    def inconsistent_subject(self):
        """Flag misspellings."""
        return False


class NHGRISample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name

    @property
    def subject_id(self):
        """Deduce id."""
        return self.attributes.attributes.participant.entityName

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        return False

    @property
    def misspelled_subject(self):
        """Flag misspellings."""
        return False

    @property
    def inconsistent_subject(self):
        """Flag misspellings."""
        return False


class NIMHSample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name

    @property
    def subject_id(self):
        """Deduce id."""
        try:
            return self.attributes.attributes.participant.entityName
        except Exception:
            return self.attributes.attributes.subject_id

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        return 'participant' not in self.attributes.attributes

    @property
    def misspelled_subject(self):
        """Flag misspellings."""
        return False

    @property
    def inconsistent_subject(self):
        """Flag misspellings."""
        return False


class PAGESample(Sample):
    """Extend Sample class."""

    def __init__(self, *args, workspace=None, blobs=None, sequencing=None, drs_file_path=None):
        """Call super."""
        super().__init__(*args, workspace=workspace, blobs=blobs, sequencing=sequencing, drs_file_path=drs_file_path)

    @property
    def id(self):
        """Deduce id."""
        return self.attributes.name

    @property
    def subject_id(self):
        """Deduce id."""
        try:
            return self.attributes.attributes.participant_id
        except Exception:
            pass
        try:
            return self.attributes.attributes.subject_id
        except Exception:
            pass
        raise Exception(f"can't find participant_id or subject_id  {self.attributes}")

    @property
    def inconsistent_entityName(self):
        """Flag entityName inconsistencies."""
        return 'participant' not in self.attributes.attributes

    @property
    def misspelled_subject(self):
        """Flag misspellings."""
        return False

    @property
    def inconsistent_subject(self):
        """Flag misspellings."""
        return False


def sample_factory(*args, **kwargs):
    """Return a specialized Subject class instance."""
    if 'CCDG' in kwargs['workspace'].name.upper():
        return CCDGSample(*args, **kwargs)
    if 'CMG' in kwargs['workspace'].name.upper():
        return CMGSample(*args, **kwargs)
    if 'GTEX' in kwargs['workspace'].name.upper():
        return GTExSample(*args, **kwargs)
    if '1000G-HIGH-COVERAGE' in kwargs['workspace'].name.upper():
        return ThousandGenomesSample(*args, **kwargs)
    if 'ANVIL_EMERGE' in kwargs['workspace'].name.upper():
        return eMERGESample(*args, **kwargs)
    if 'NHGRI' in kwargs['workspace'].name.upper():
        return NHGRISample(*args, **kwargs)
    if 'NIMH' in kwargs['workspace'].name.upper():
        return NIMHSample(*args, **kwargs)
    if 'PAGE' in kwargs['workspace'].name.upper():
        return PAGESample(*args, **kwargs)
    raise Exception(f'Not implemented {kwargs["workspace"].name.upper()}')
