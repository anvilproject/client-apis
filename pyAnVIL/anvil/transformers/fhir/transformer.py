"""Transform terra workspaces to FHIR."""
from anvil.transformers.fhir.document_reference import DocumentReference
from anvil.transformers.fhir.patient import Patient
from anvil.transformers.fhir.research_study import ResearchStudy
from anvil.transformers.fhir.research_subject import ResearchSubject
from anvil.transformers.fhir.specimen import Specimen
from anvil.transformers.transformer import Transformer
import types


class FhirTransformer(Transformer):
    """Represent terra entities in Fhir.  Transform with .entity() method."""

    def __init__(self, *args, **kwargs):
        """Transform entities."""
        super(FhirTransformer, self).__init__(*args, **kwargs)

    # overrides
    def transform_workspace(self, workspace):
        """Transform workspace."""
        def entity(self):
            yield ResearchStudy.build_entity(self.attributes.workspace.attributes)
        workspace.entity = types.MethodType(entity, workspace)
        yield workspace

    def transform_subject(self, subject):
        """Transform subject."""
        def entity(self):
            yield ResearchSubject.build_entity(self)
            yield Patient.build_entity(self)
        subject.entity = types.MethodType(entity, subject)
        yield subject

    def transform_sample(self, sample):
        """Transform sample."""
        def entity(self):
            yield Specimen.build_entity(self)
        sample.entity = types.MethodType(entity, sample)
        yield sample

    def transform_blob(self, blob):
        """Transform blob (noop)."""
        def entity(self):
            yield DocumentReference.build_entity(self)
        blob.entity = types.MethodType(entity, blob)
        yield blob
