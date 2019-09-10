from attrdict import AttrDict
from . import BaseApp, strip_all


class CMG(BaseApp):
    """Transforms CMG to cannonical graph."""

    def __init__(self, project_pattern='^AnVIL.*CMG.*$', *args, **kwargs):
        """Initializes class variables."""
        super(CMG, self).__init__(project_pattern=project_pattern, **kwargs)

    def get_terra_participants(self):
        """Cleans up terra participants, maps family."""
        for p in super().get_terra_participants():
            p = AttrDict(harmonize_participant(p))
            yield p

    def get_terra_samples(self):
        """Cleans up terra samples."""
        for s in super().get_terra_samples():
            yield AttrDict(harmonize_sample(s))

    def to_graph(self):
        """Adds Family, Demographic to graph"""
        all_subjects = [subject for subject in self.get_terra_participants()]
        family_lookup = {p['individual_id']: p['submitter_id'] for p in all_subjects if 'individual_id' in p}
        families = set([p['family_id'] for p in all_subjects if p.family_id])

        phenotypes = set([participant['phenotype_id'] for participant in all_subjects if 'phenotype_id' in participant]) | \
            set([participant['present'] for participant in all_subjects if 'present' in participant]) | \
            set([participant['absent'] for participant in all_subjects if 'absent' in participant])
        phenotypes = phenotypes - set([None, 'n/a'])

        genes = set([participant['gene'] for participant in all_subjects if 'gene' in participant])
        genes = genes - set([None, 'n/a'])

        G = super().to_graph()

        for phenotype in phenotypes:
            G.add_node(phenotype, label='Phenotype', submitter_id=phenotype, project_id='cmg*')

        for gene in genes:
            G.add_node(gene, label='Gene', submitter_id=gene, project_id='cmg*')

        for family in families:
            G.add_node(family, label='Family', submitter_id=family, project_id='cmg*')

        for subject in all_subjects:
            if subject.family_id:
                G.add_edge(subject.submitter_id, subject.family_id, label='member_of')
            if 'paternal_id' in subject and subject.paternal_id in family_lookup:
                G.add_edge(subject.submitter_id, family_lookup[subject.paternal_id], label='father')
                G.add_edge(family_lookup[subject.paternal_id], subject.submitter_id, label='child')
            if 'maternal_id' in subject and subject.maternal_id in family_lookup:
                G.add_edge(subject.submitter_id, family_lookup[subject.maternal_id], label='mother')
                G.add_edge(family_lookup[subject.maternal_id], subject.submitter_id, label='child')
            if subject.phenotype_id:
                diagnosis_id = '{}/{}'.format(subject.submitter_id, subject.phenotype_id)
                G.add_node(diagnosis_id, label='Diagnosis', project_id=subject.project_id)
                G.add_edge(subject.submitter_id, diagnosis_id, label=subject.affected_status)
                G.add_edge(diagnosis_id, subject.phenotype_id, label='instance_of')
            if subject.demographic:
                G.add_node(subject.demographic.submitter_id, label='Demographic', project_id=subject.project_id)
                G.add_edge(subject.submitter_id, subject.demographic.submitter_id, label='described_by')
            if subject.gene:
                G.add_edge(subject.submitter_id, subject.gene, label='expressed')

        self.G = G
        return self.G


def harmonize_sample(s):
    """Ensures links."""
    s.participant = participant_id(s)
    s.submitter_id = f'{participant_id(s)}-sample'
    return s


def participant_id(s):
    """Corrects mispelling."""
    if 'participant' in s:
        if isinstance(s.participant, str):
            return s.participant
        return s.participant.entityName
    if 'participent' in s:
        return s.participent


def harmonize_participant(p):
    return {
        'project_id': p.project_id,
        'maternal_id': strip_all(p.get('04_Maternal_ID', None)),
        'paternal_id': strip_all(p.get('03_Paternal_ID', None)),
        'subject_id': p.get('Subject_ID', p.get('SUBJECT_ID', None)),
        'submitter_id': p['submitter_id'],
        'family_id': strip_all(p.get('01_Family_ID', None)),
        'individual_id': strip_all(p.get('02_Individual_ID', None)),
        'phenotype_id': strip_all(p.get('07_Phenotype', None)),
        'affected_status': strip_all(p.get('06_Affected_Status', None)),
        'present': strip_all(p.get('08_HPO_Terms_Present', None)),
        'absent': strip_all(p.get('09_HPO_Terms_Absent', None)),
        'gene': strip_all(p.get('10_Gene-1', None)),
        'demographic': map_demographic(p)
    }


def map_demographic(p):
    return AttrDict({
        'gender': p.get('gender', None),
        'submitter_id': f'{p["submitter_id"]}_demographic',
        'project_id': p['project_id']
    })
