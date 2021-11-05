"""Test transformers."""

from anvil.transformers.transformer import Transformer
from anvil.transformers.fhir.transformer import FhirTransformer
from anvil.util.reconciler import Reconciler
import logging

from anvil.terra.workspace import Workspace
from anvil.terra.subject import Subject
from anvil.terra.sample import Sample
from anvil.terra.blob import Blob

import json

logging.getLogger('anvil.test_transformers').setLevel(logging.DEBUG)
logger = logging.getLogger('anvil.test_transformers')


def test_transformer(user_project, namespaces, avro_path, terra_output_path, drs_output_path, project_pattern='AnVIL_CMG_Broad_Muscle_KNC_WGS'):
    """Ensure terra."""
    reconciler = Reconciler('CMG', user_project, namespaces, project_pattern, avro_path, terra_output_path, drs_output_path)
    assert reconciler, "MUST create reconciler"
    assert len(reconciler.workspaces) == 1, "MUST have at least expected number of workspaces"
    for workspace in reconciler.workspaces:
        transformer = Transformer(workspace=workspace)
        items = [item for item in transformer.transform()]
        assert len(items) > 0, "Should produce at least one item."
        assert len([w for w in items if isinstance(w, Workspace)]), "Should have an instance of Workspace"
        assert len([s for s in items if isinstance(s, Subject)]), "Should have an instance of Subject"


def test_emitter(user_project, namespaces, avro_path, terra_output_path, drs_output_path, project_pattern='AnVIL_CMG_Broad_Muscle_KNC_WGS'):
    """Ensure terra."""
    reconciler = Reconciler('CMG', user_project, namespaces, project_pattern, avro_path, terra_output_path, drs_output_path)

    classes = [Workspace, Subject, Sample, Blob]
    emitters = {c.__name__: open(f"/tmp/{c.__name__}.json", "w") for c in classes}

    def get_emitter(item):
        for c in classes:
            if isinstance(item, c):
                return emitters[c.__name__]
        return None

    for workspace in reconciler.workspaces:
        transformer = FhirTransformer(workspace=workspace)
        for item in transformer.transform():
            emitter = get_emitter(item)
            assert emitter, f"Should have an emitter {item}"
            for entity in item.entity():
                json.dump(entity, emitter, separators=(',', ':'))
                emitter.write('\n')

    for emitter in emitters.values():
        emitter.close()

    emitters = {c.__name__: open(f"/tmp/{c.__name__}.json", "r") for c in classes}
    for emitter in emitters.values():
        for line in emitter.readlines():
            entity = json.loads(line)
            assert entity
            # for k, v in entity.items():
            #     assert not isinstance(v, list), f"Should be scalar {k}"
            #     assert not isinstance(v, dict), f"Should be scalar {k}"
