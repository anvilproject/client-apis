"""How to read avro."""

from fastavro import reader
from pprint import pprint
path = "/Users/walsbr/Downloads/export_2020-11-04T17_48_47.avro"
with open(path, 'rb') as fo:
    for record in reader(fo):
        schema = record
        break

pprint(schema)

# entities = schema['object']['nodes']

# for entity in entities:
#     for link in entity['links']:
#         print(entity['name'], link['dst'])


path = "/Users/walsbr/Downloads/export_2020-11-04T17_48_47.avro"
limit = 10
with open(path, 'rb') as fo:
    for record in reader(fo):
        pprint(record)
        limit -= 1
        if limit == 0:
            break
