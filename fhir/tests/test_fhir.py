"""Test AnVIL FHIR."""

import logging
from anvil.terra.reconciler import Reconciler
from anvil.transformers.fhir.transformer import FhirTransformer

logger = logging.getLogger(__name__)


def test_configuration(debug_caplog, load_configurations, config):
    """Verify config loaded."""
    for url in load_configurations:
        response = config.connection.get(url)
        logger.debug(f"test_configuration {url} {response.status_code} {response.text}")
        assert response.ok, f"{url} {response.status_code} {response.text}"


def test_transformers(load_configurations, config, user_project, namespaces):
    """Transform and load."""
    project_pattern = 'AnVIL_CMG_Broad_Muscle_KNC_WGS'
    reconciler = Reconciler('CMG', user_project, namespaces, project_pattern)

#   "resourceType": "ResearchStudy",
#   "id": "CMG-Broad-Muscle-KNC-WGS",
    for workspace in reconciler.workspaces:
        transformer = FhirTransformer(workspace=workspace)
        for item in transformer.transform():
            for entity in item.entity():
                resourceType = entity['resourceType']
                id = entity['id']
                url = f"{config.base_url}/{resourceType}/{id}"
                response = config.connection.put(
                    url=url,
                    json=entity,
                )
                assert response.ok, f"body:{entity}\nerror: {response.text}"
                response_body = response.json()
                logger.debug(f"created {resourceType}/{response_body['id']} at {url}")


# class Emitters:

#     def __init__(self, base_path="/tmp"):
#         self._lookup = {}
#         self._base_path = base_path

#     def get(self, key):
#         emitter = self._lookup.get(key, None)
#         if not emitter:
#             self._lookup[key] = open(f"{self._base_path}/{key}.json", "w")
#         self._lookup[key]

#     def close(self):
#         for emitter in self._lookup.values():
#             emitter.close()
