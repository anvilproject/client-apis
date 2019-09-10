from attrdict import AttrDict
import os
from . import BaseApp


class GTEx(BaseApp):
    """Transforms GTEx to cannonical graph."""

    def __init__(self, project_pattern='^AnVIL_GTEx_V8_hg38$', *args, **kwargs):
        """Initializes class variables."""
        super(GTEx, self).__init__(project_pattern=project_pattern, **kwargs)

    def sample_submitter_id(self, sample):
        """Creates a sample submitter_id."""
        return '{}/{}/{}'.format(
            sample.project_id,
            sample.participant.entityName,
            sample.tissue_id
        )

    def get_terra_participants(self):
        """Adds files dict to participant."""
        for p in super().get_terra_participants():
            files = {}
            for k, v in p.items():
                if self.is_file(v) and self.file_type(v):
                    files[k] = AttrDict({'path': v, 'type': self.file_type(v)})
                if self.file_md5(v):
                    md5_filename, md5_file_extension = os.path.splitext(v)
                    for k, f in files.items():
                        if f.path == md5_filename:
                            f.md5 = v
            p.files = files
            yield p

    def to_graph(self):
        """Adds Subject.files and Demographic to graph"""
        graph = super().to_graph()

        for subject in self.get_terra_participants():
            graph.add_node(f'{subject.submitter_id}-demographic', label='Demographic', age=subject.age, sex=subject.sex, project_id=subject.project_id)
            graph.add_edge(subject.submitter_id, f'{subject.submitter_id}-demographic', label='described_by')
            for k, file in subject.files.items():
                type = file.type.replace('.', '').capitalize()
                graph.add_node(file.path, label=f'{type}File', project_id=subject.project_id)
                if 'counts' in k:
                    type = 'counts'
                graph.add_edge(subject.submitter_id, file.path, label=type)

        self.G = graph
        return self.G
