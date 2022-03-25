import logging
from pprint import pprint
from anvil.etl.utilities.entities import Entities
from extractors.terra import bucket_fields

from google.cloud.storage import Client
from google.cloud.storage.blob import Blob


logger = logging.getLogger(__name__)

from .terra import extract_bucket_fields
 
# @cli.command('clean')
# @click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
# def clean(output_path):
#     """Remove database."""
#     path = f"{output_path}/google_entities.sqlite"
#     try:
#         os.remove(path)
#         logger.info(('removed', path))
#     except OSError as e:
#         logger.warning((e, path))


def extract_buckets(output_path, user_project):
    entities = Entities(path=f"{output_path}/google_entities.sqlite")
    client = Client(project=user_project)
    bucket_fields = [bf for bf in extract_bucket_fields(output_path)]
    already_done = set()
    for bucket_field in bucket_fields:
        for bucket in bucket_field['buckets']:
            if bucket in already_done:
                continue
            logger.info((bucket_field['consortium_name'],bucket_field['workspace_name'], bucket))
            already_done.add(bucket)
            blobs = client.list_blobs(bucket)
            for blob in blobs:
                _properties = dict(blob._properties)
                _properties['path'] = blob.path
                _properties['public_url'] = blob.public_url
                # _properties['url'] = f"gs://{blob.path_helper(_properties['bucket'], _properties['name'])}".replace('/o/', '/')
                _properties['url'] = f"gs://{_properties['bucket']}/{_properties['name']}"
                entities.put(key=_properties['url'], label='Blob', data=_properties)
            entities.commit()        
    entities.index()

