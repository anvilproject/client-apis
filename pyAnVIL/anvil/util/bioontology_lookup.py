import os
import json
import requests

REST_URL = "http://data.bioontology.org"

def _get_json(url, api_key):
    response = requests.get(url, headers={'Authorization': f'apikey token={api_key}'})
    response.raise_for_status()
    assert isinstance(response.json(), dict), f"{url} returned {response.text} ?"
    return response.json()


def lookup_term(term, api_key, ontology_preferences=['MONDO', 'DOID']):
    """Generator, return classes that match ontology_preferences."""
    assert api_key, "Please provide an API key.  See https://bioportal.bioontology.org/help#Getting_an_API_key"
    url = f"{REST_URL}/search?q={term}"
    results = _get_json(url, api_key)
    from pprint import pprint
    for ontology_preference in ontology_preferences:
        for item in results['collection']:
            if ontology_preference in item['@id']:
                yield (ontology_preference, item)


