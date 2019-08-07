# setup

From a terminal session:


```
# install dependencies
pip install upsetplot

# ensure gcloud installed: see https://cloud.google.com/sdk/docs/quickstart-linux
gcloud --version

# install our nacent client lib

git clone https://github.com/anvilproject/client-apis
cd client-apis/
git checkout pyAnVIL
pip install -e .

# execute the command line, it will give you a oauth prompt to login to terra
python  pyAnVIL/anvil/cli.py


```


From there, the python notebook in this directory explores the schemas, [gist](https://gist.github.com/bwalsh/57e6225ef6018e221fd0a566c2d1d753) 

e.g.

```
from anvil import anvil
import sys

def projects_with_schemas(namespace='anvil-datastorage', project_pattern='AnVIL.*CMG.*'):
    """Should return projects."""
    # get all matching projects in the namespace
    projects = anvil.get_projects([namespace], project_pattern=project_pattern)
    assert len(projects) > 0, "Should have at least 1 project in {} matching {}".format(namespace, project_pattern)
    # add the project schema
    projects = [anvil.get_project_schema(p) for p in projects]
    for p in projects:
        if len(p.schema.keys()) == 0:
            print('{} missing schema'.format(p.project), file=sys.stderr)
    # trim projects without schemas        
    projects = [p for p in projects if len(p.schema.keys()) > 0]
    return projects
        
projects = projects_with_schemas()

print([p.project for p in projects])

    
```    

