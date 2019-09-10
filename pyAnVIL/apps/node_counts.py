import pandas as pd
import numpy as np
from io import StringIO


def create_table(node_counts, path='notebooks/figures/node_counts.html'):
    """Creates a markdown table of node counts in path."""
    output = StringIO()
    df = pd.DataFrame.from_records(node_counts, index=['source', 'project_id']).replace(np.nan, '', regex=True)
    df.to_html(output)
    contents = output.getvalue()
    output.close()
    return contents
