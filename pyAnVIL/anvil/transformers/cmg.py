from attrdict import AttrDict
from . import BaseApp, strip_all


class CMG(BaseApp):
    """Transforms CMG to cannonical graph."""
    def __init__(self, project_pattern='^AnVIL.*CMG.*$', *args, **kwargs):
        """Initializes class variables."""
        super(CMG, self).__init__(project_pattern=project_pattern, **kwargs)

    def is_blacklist(self, project_id):
        """Returns true if blacklisted"""
        return 'AnVIL_CMG_Broad_Muscle_KNC_WGS' in project_id


    def get_terra_participants(self):
        """Cleans up terra participants, maps family."""
        for p in super().get_terra_participants():
            p = AttrDict(harmonize_participant(p))
            yield p

    def get_terra_samples(self):
        """Cleans up terra samples."""
        for s in super().get_terra_samples():
            harmonized_sample = self.harmonize_sample(s)
            if harmonized_sample.participant is None and not self.reported_already:
                self.reported_already = True
                self.logger.warn(f"sample missing participant value, ignoring. {s.project_id} {s.get('root_sample_id', 'missing sample id')}")
                continue
            yield AttrDict(self.harmonize_sample(s))

    def get_terra_sequencing(self):
        """Returns generator with samples associated with projects."""
        self.logger.info('get_terra_sequencing')
        for p in self.get_terra_projects():
            if 'sequencing' in p:
                for s in p.sequencing:
                    yield s

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

        for sequence in self.get_terra_sequencing():
            G.add_node(sequence.submitter_id, label='Sequencing', project_id=subject.project_id)
            # assert 'collaborator_sample_id' in sequence, sequence
            sample_id = sequence.get('collaborator_sample_id', sequence.submitter_id)
            G.add_edge(sequence.submitter_id, sample_id, label='representation_of')
            for k, file in sequence.files.items():
                file = AttrDict(file)
                type = file.type.replace('.', '').capitalize()
                G.add_node(file.path, label=f'{type}File', project_id=sequence.project_id, size=file.size, time_created=file.time_created)
                G.add_edge(sequence.submitter_id, file.path, label=type)


        self.G = G
        return self.G

    def harmonize_sample(self, s):
        """Ensures links."""
        s.participant = self.participant_id(s)
        if '02-sample_id' in s:
            s.submitter_id = s['02-sample_id']
        s.submitter_id = f'{self.participant_id(s)}-sample'
        return s

    def participant_id(self, s):
        """Corrects misspelling, mismatches, and schema drift."""
        if '01-subject_id' in s:
            return s['01-subject_id']
        if 'collaborator_participant_id' in s:
            return s.collaborator_participant_id
        if 'participant_id' in s:
            return s.participant_id
        if 'participant' in s:
            if s.participant is None:
                return None
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
