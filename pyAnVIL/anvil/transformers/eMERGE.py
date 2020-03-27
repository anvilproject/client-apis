from . import BaseApp


class eMERGE(BaseApp):
    """Transforms eMERGE to cannonical graph."""

    def __init__(self, project_pattern='^.*_Emerge_.*$', *args, **kwargs):
        """Initializes class variables."""
        super(eMERGE, self).__init__(project_pattern=project_pattern, **kwargs)
