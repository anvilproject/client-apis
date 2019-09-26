import os
from attrdict import AttrDict
from anvil import terra
import firecloud.api as FAPI
import networkx as nx
import logging
# import datetime


class BaseApp():
    """The base class for terra to cannonical graph transformers."""

    def __init__(self, project_pattern=None, program='anvil-datastorage', fapi=FAPI, user_project=None):
        """Initializes workspace"""
        assert project_pattern, 'Please set project_pattern'
        assert program, 'Please set program'
        assert user_project, 'Please set user_project'
        self.program = program
        self.project_pattern = project_pattern
        self.fapi = fapi
        self.projects = None
        self.user_project = user_project
        self.G = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_terra_projects(self):
        """Returns project with associated schema."""
        if self.projects:
            return self.projects
        projects = terra.get_projects([self.program], project_pattern=self.project_pattern, fapi=self.fapi, user_project=self.user_project)
        assert len(projects) > 0, f"Should have at least 1 project in {self.program} matching {self.project_pattern}"
        blob_sum = sum([len(p.blobs) for p in projects])
        assert blob_sum > 0, f"Should have at least more than 0 blobs {self.program}"
        self.logger.info(f'number of blobs in {self.project_pattern}: {blob_sum}')

        # add the project schema
        projects = [terra.get_project_schema(p, fapi=self.fapi) for p in projects]
        self.projects = []
        for p in projects:
            if len(p.schema.keys()) == 0:
                self.logger.warning(f'{p.project} missing schema, project will not be included.')
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
            print(f"start get_entities ")
            samples = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='sample', fapi=self.fapi)
            assert len(samples) == p.schema.sample.count, f"Retrieved samples entities count {len(samples)} did not match anticipated count in schema {p.schema.sample.count}"
            print(f"end get_entities")
            print(f'{p.program} {p.project} {len(p.blobs)} {len(samples)}')
            for sample in samples:
                if 'attributes' not in sample:
                    continue
                attributes = sample.attributes
                attributes.project_id = p.project_id
                attributes.submitter_id = self.sample_submitter_id(attributes)
                attributes.files = self.identify_files(attributes, p.blobs)
                yield attributes

    def identify_files(self, sample, blobs):
        """Returns a dictionary of files associated with this sample."""
        files = {}
        for k, v in sample.items():
            if k not in ['cram', 'md5_path', 'crai_path', 'cram_path', 'bam', 'bam_path', 'ase_wasp_counts',
                         'ase_counts', 'cram_file', 'cram_index', 'wgs_cram_index', 'wgs_cram_file',
                         'wes_bam_index', 'wes_bam_file', 'ase_wasp_chrX_raw_counts', 'ase_chrX_raw_counts']:
                continue

            filename, file_type = self.file_type(v)
            if 'md5' not in file_type:
                blob = blobs.get(v, None)
                size = blob['size'] if blob else 0
                if size == 0:
                    print(f"{v} has no blob!")
                files[k] = AttrDict({'path': v, 'type': file_type, 'size': size})
            else:
                for k, f in files.items():
                    if f.path == filename:
                        f.md5 = v
        return files

    # def file_md5(self, path):
    #     """Returns path if file is an md5 container, None otherwise"""
    #     filename, file_extension = os.path.splitext(path)
    #     if file_extension == '.md5':
    #         return path
    #     return None

    def file_type(self, path):
        """Returns filename, file_extension ignores gz extension"""
        filename, file_extension = os.path.splitext(path)
        if file_extension == '.gz':
            file_extension = filename.split('.')[-1]
        return filename, file_extension

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
        print('to_graph: get_terra_projects')
        for project in self.get_terra_projects():
            G.add_node(project.project_id, label='Project', project_id=project.project_id)
        print('to_graph: get_terra_participants')
        # start = datetime.datetime.now()
        for subject in self.get_terra_participants():
            # end = datetime.datetime.now()
            # print('got participant', end-start)
            # start = end
            assert subject.submitter_id, 'should have submitter_id'
            assert subject.project_id, 'should have project_id'
            G.add_node(subject.submitter_id, label='Subject', **subject)
            G.add_edge(subject.submitter_id, subject.project_id, label='member_of')
        print('to_graph: get_terra_samples')
        # start = datetime.datetime.now()
        for sample in self.get_terra_samples():
            # end = datetime.datetime.now()
            # print('got sample', end-start)
            # start = end
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
                G.add_node(file.path, label=f'{type}File', project_id=sample.project_id, size=file.size)
                G.add_edge(sample.submitter_id, file.path, label=type)
            # end = datetime.datetime.now()
            # print('added nodes', end-start)
            # start = end

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
