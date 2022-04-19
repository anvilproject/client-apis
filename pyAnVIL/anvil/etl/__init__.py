import os

DEFAULT_OUTPUT_PATH = os.environ.get('OUTPUT_PATH','./DATA')

DEFAULT_GEN3_CREDENTIALS_PATH = os.path.expanduser('~/.gen3/credentials.json')

DEFAULT_GOOGLE_PROJECT = os.environ.get('GOOGLE_PROJECT', None)