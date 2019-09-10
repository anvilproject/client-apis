from . import BaseApp


class ThousandGenomes(BaseApp):
    """Transforms 1000G to cannonical graph."""

    def __init__(self, project_pattern='^1000G-high-coverage-2019$', *args, **kwargs):
        """Initializes class variables."""
        super(ThousandGenomes, self).__init__(project_pattern=project_pattern, **kwargs)

    def to_graph(self):
        """Adds population vertex and edge to graph."""
        graph = super().to_graph()
        for subject in self.get_terra_participants():
            graph.add_node(subject.POPULATION, label='Population', submitter_id=subject.POPULATION, project_id=subject.project_id)
            graph.add_edge(subject.submitter_id, subject.project_id, label='member_of')
        self.G = graph
        return self.G
