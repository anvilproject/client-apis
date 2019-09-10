from . import BaseApp, strip_all
from attrdict import AttrDict


class CCDG(BaseApp):
    """Transforms CCDG to cannonical graph."""

    def __init__(self, project_pattern='^AnVIL.*CCDG.*$', *args, **kwargs):
        """Initializes class variables."""
        super(CCDG, self).__init__(project_pattern=project_pattern, **kwargs)

    def get_terra_participants(self):
        """Cleans up terra participants, maps family."""
        self.families = {}
        for p in super().get_terra_participants():
            p = AttrDict(harmonize_participant(p))
            self.add_to_families(p)
            yield p

    def get_terra_samples(self):
        """Cleans up terra samples."""
        for s in super().get_terra_samples():
            yield AttrDict(harmonize_sample(s))

    def add_to_families(self, p):
        families = self.families
        if p.family_id not in families:
            families[p.family_id] = AttrDict(project_id=p.project_id)
        if 'sibling' not in families[p.family_id]:
            families[p.family_id]['sibling'] = []
        if p.family_relationship is None and p.mother and p.father:
            families[p.family_id]['mother'] = [p.mother]
            families[p.family_id]['father'] = [p.father]
            families[p.family_id]['sibling'].append(p.submitter_id)
            return
        if p.family_relationship is None:
            return
        if p.family_relationship not in families[p.family_id]:
            families[p.family_id][p.family_relationship] = []
        families[p.family_id][p.family_relationship].append((p.submitter_id))

    def to_graph(self):
        """Adds Family, Demographic to graph"""
        G = super().to_graph()
        for subject in self.get_terra_participants():
            G.add_node(subject.demographic.submitter_id, label='Demographic', **subject.demographic)
            G.add_edge(subject.submitter_id, subject.demographic.submitter_id, label='described_by')
            if 'diagnosis' in subject and subject.diagnosis:
                G.add_node(subject.diagnosis, label='Diagnosis', project_id=subject.project_id)
                G.add_edge(subject.submitter_id, subject.diagnosis, label='has')

        for family_id, family in self.families.items():
            G.add_node(family_id, label='Family', project_id=family.project_id)
            family_relationships = [k for k in family if k != 'project_id']
            for family_relationship in family_relationships:
                for member in family[family_relationship]:
                    G.add_edge(member, family_id, label='member_of')
                proband = family.get('proband', [None])[0]
                if proband:
                    for member in family.get('father', []):
                        G.add_edge(member, proband, label='father')
                        G.add_edge(proband, member, label='child')
                    for member in family.get('mother', []):
                        G.add_edge(member, proband, label='mother')
                        G.add_edge(proband, member, label='child')
                siblings = family.get('sibling', [])
                for member in siblings:
                    other_siblings = [s for s in siblings if s != member]
                    for s in other_siblings:
                        G.add_edge(member, s, label='sibling')

        self.G = G
        return self.G


def harmonize_sample(s):
    s.participant = participant_id(s)
    s.submitter_id = f'{participant_id(s)}-sample'
    return s


def participant_id(s):
    if 'participant' in s:
        if isinstance(s.participant, str):
            return s.participant
        return s.participant.entityName
    if 'participent' in s:
        return s.participent


def harmonize_participant(p):
    return {
        'project_id': p.project_id,
        'mother': strip_all(strip_all(p.get('MOTHER', p.get('Mother_ID', None)))),
        'father': strip_all(strip_all(p.get('FATHER', p.get('Father_ID', None)))),
        'subject_id': p.get('Subject_ID', p.get('SUBJECT_ID', None)),
        'submitter_id': p['submitter_id'],
        'family_id': None if p.get('Family_ID', p.get('FAMILY_ID', None)) is None else p.get('Family_ID', p.get('FAMILY_ID', None)) + '_family',
        'family_relationship': harmonize_family_relationship(p.get('Family_Relationship', None)),
        'gender': strip_all(p.get('Gender', p.get('Sex', None))),
        'collaborator_participant_id': p.get('SOURCE_SUBJECT_ID', p.get('collaborator_participant_id', None)),
        'age': strip_all(p.get('AGE', p.get('Age', None))),
        'age_baseline': strip_all(p.get('AGE_baseline', p.get('AGE_baseline', None))),
        'age_disease': strip_all(p.get('Age_Disease', p.get('AGE_disease', None))),
        'ethnicity': strip_all(p.get('RACE', p.get('Race_Ethnicity', None))),
        'center': strip_all(p.get('SUBJECT_SOURCE', p.get('Center', None))),
        'iq': strip_all(p.get('IQ', None)),  # all empty
        'diagnosis': strip_all(strip_all(p.get('ADI-R_DIAG', None))),
        'Diagnosed_co-morbidity': strip_all(strip_all(p.get('Diagnosed_co-morbidity', None))),  # all empty
        'AFFECTION_STATUS': strip_all(strip_all(p.get('AFFECTION_STATUS', None))),
        'target_coverage': strip_all(strip_all(p.get('TargetCoverage', p.get('Target_Coverage', None)))),
        'CRAM': strip_all(strip_all(p.get('CRAM', None))),
        'freeze': strip_all(strip_all(p.get('freeze', p.get('freeze', None)))),
        'disease_status': strip_all(strip_all(p.get('Disease_Status', None))),
        'demographic': map_demographic(p)
    }


def harmonize_family_relationship(v):
    if not v:
        return v
    v = v.lower()
    if v in ['brother or sister', 'brother', 'sister']:
        v = 'sibling'
    for s in ['twin', 'sibling', 'brother', 'sister']:
        if s in v:
            v = 'sibling'
            break
    if v in ['son or daughter', 'son', 'daughter']:
        v = 'child'

    if v not in ['sibling', 'mother', 'father', 'proband', 'child']:
        v = 'other'

    return v


def map_demographic(p):
    return AttrDict({
        'gender': p.get('gender', None),
        'iq': p.get('iq', None),
        'age': p.get('age', None),
        'age_baseline': p.get('age_baseline', None),
        'age_disease': p.get('age_disease', None),
        'submitter_id': f'{p["submitter_id"]}_demographic',
        'project_id': p['project_id']
    })
