import os
import sys
from attrdict import AttrDict
from anvil import terra
import firecloud.api as FAPI
import networkx as nx


class BaseApp():
    """The base class for terra to cannonical graph transformers."""

    def __init__(self, project_pattern=None, program='anvil-datastorage', fapi=FAPI):
        """Initializes workspace"""
        assert project_pattern, 'Please set project_pattern'
        assert program, 'Please set program'
        self.program = program
        self.project_pattern = project_pattern
        self.fapi = fapi
        self.projects = None
        self.G = None

    def get_terra_projects(self):
        """Returns project with associated schema."""
        if self.projects:
            return self.projects
        projects = terra.get_projects([self.program], project_pattern=self.project_pattern, fapi=self.fapi)
        assert len(projects) > 0, f"Should have at least 1 project in {self.program} matching {self.project_pattern}"
        # add the project schema
        projects = [terra.get_project_schema(p, fapi=self.fapi) for p in projects]
        self.projects = []
        for p in projects:
            if len(p.schema.keys()) == 0:
                print(f'{p.project} missing schema', file=sys.stderr)
            else:
                self.projects.append(p)
        return self.projects

    def get_terra_participants(self):
        """Returns generator with participants associated with projects."""
        for p in self.get_terra_projects():
            participants = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='participant', fapi=self.fapi)
            assert len(participants) == p.schema.participant.count, f"Retrieved participants entities count {len(participants)} did not match anticipated count in schema {p.schema.participant.count}"
            for participant in participants:
                attributes = participant.attributes
                attributes.submitter_id = participant.name
                attributes.project_id = p.project_id
                yield attributes

    def sample_submitter_id(self, sample):
        """Creates a sample submitter_id."""
        return '{}/{}'.format(
            sample.project_id,
            sample.get('sample_alias', sample.get('collaborator_sample_id'))
        )

    def get_terra_samples(self):
        """Returns generator with samples associated with projects."""
        for p in self.get_terra_projects():
            samples = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='sample', fapi=self.fapi)
            assert len(samples) == p.schema.sample.count, f"Retrieved samples entities count {len(samples)} did not match anticipated count in schema {p.schema.sample.count}"
            for sample in [sample for sample in samples if 'attributes' in sample]:
                attributes = sample.attributes
                attributes.project_id = p.project_id
                attributes.submitter_id = self.sample_submitter_id(attributes)
                attributes.files = self.identify_files(attributes)
                yield attributes

    def identify_files(self, sample):
        """Returns a dictionary of files associated with this sample."""
        files = {}
        for k, v in sample.items():
            if self.is_file(v) and self.file_type(v):
                files[k] = AttrDict({'path': v, 'type': self.file_type(v)})
            if self.file_md5(v):
                md5_filename, md5_file_extension = os.path.splitext(v)
                for k, f in files.items():
                    if f.path == md5_filename:
                        f.md5 = v
        return files

    def file_md5(self, path):
        """Returns path if file is an md5 container, None otherwise"""
        if not self.is_file(path):
            return None
        filename, file_extension = os.path.splitext(path)
        if file_extension == '.md5':
            return path
        return None

    def file_type(self, path):
        """Returns file_extension if path is anything other than md5 container, None otherwise"""
        if not self.is_file(path):
            return None
        filename, file_extension = os.path.splitext(path)
        if file_extension == '.md5':
            return None
        if file_extension == '.gz':
            return filename.split('.')[-1]
        return file_extension

    def is_file(self, path):
        """Returns True if path is in google storage, False otherwise"""
        if isinstance(path, str) and path.startswith('gs://'):
            return True
        return False

    def to_graph(self):
        """Creates a simple networkx graph."""
        if self.G:
            return self.G
        G = nx.MultiDiGraph()
        for project in self.get_terra_projects():
            G.add_node(project.project_id, label='Project', project_id=project.project_id)
        for subject in self.get_terra_participants():
            assert subject.submitter_id, 'should have submitter_id'
            assert subject.project_id, 'should have project_id'
            G.add_node(subject.submitter_id, label='Subject', **subject)
            G.add_edge(subject.submitter_id, subject.project_id, label='member_of')
        for sample in self.get_terra_samples():
            assert sample.submitter_id, 'should have submitter_id'
            assert sample.project_id, 'should have project_id'
            assert sample.participant, 'should have participant'
            participant = sample.participant
            if not isinstance(participant, str):
                participant = participant['entityName']
            G.add_node(sample.submitter_id, label='Sample', project_id=sample.project_id)
            G.add_edge(participant, sample.submitter_id, label='drawn_from')
            for k, file in sample.files.items():
                type = file.type.replace('.', '').capitalize()
                G.add_node(file.path, label=f'{type}File', project_id=sample.project_id)
                G.add_edge(sample.submitter_id, file.path, label=type)
        self.G = G
        return self.G

    def graph_node_counts(self):
        """Returns a dict of node counts by project/label."""
        counts = {}
        graph = self.to_graph()
        for n in graph.nodes():
            assert 'label' in graph.node[n], f'{n} has no label?'
            label = graph.node[n]['label']
            assert 'project_id' in graph.node[n], f'{label}({n}) has no project_id?'
            project_id = graph.node[n]['project_id']
            project_counts = counts.get(project_id, {})
            label_counts = project_counts.get(label, 0) + 1
            project_counts[label] = label_counts
            counts[project_id] = project_counts
        return counts


def strip_NA(v):
    """Cleans up."""
    if v in ['NA', '#N/A', 'N/A', 'n/a']:
        return None
    return v


def strip_0(v):
    """Cleans up."""
    if v in [0, '0']:
        return None
    return v


def strip_all(v):
    """Cleans up."""
    return strip_0(strip_NA(v))
