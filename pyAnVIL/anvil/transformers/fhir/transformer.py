"""Transform terra workspaces to FHIR."""
# from anvil.transformers.fhir.attachment import Attachment
from anvil.terra.blob import Blob
from anvil.transformers.fhir.document_reference import DocumentReference
from anvil.transformers.fhir.observation import DiseaseObservation
from anvil.transformers.fhir.organization import Organization
from anvil.transformers.fhir.patient import Patient
from anvil.transformers.fhir.practitioner import Practitioner
from anvil.transformers.fhir.research_study import ResearchStudy
from anvil.transformers.fhir.research_subject import ResearchSubject
from anvil.transformers.fhir.specimen import Specimen
from anvil.transformers.fhir.task import SpecimenTask
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
            practitioner = Practitioner.build_entity(self)
            if practitioner:
                yield practitioner
            organization = Organization.build_entity(self)
            if organization:
                yield organization
            yield ResearchStudy.build_entity(self)
        workspace.entity = types.MethodType(entity, workspace)
        yield workspace

    def transform_subject(self, subject):
        """Transform subject."""
        def entity(self):
            yield Patient.build_entity(self)
            yield ResearchSubject.build_entity(self)
            if self.diseases:
                for d in self.diseases:
                    yield DiseaseObservation.build_entity(self, disease=d)

        subject.entity = types.MethodType(entity, subject)
        yield subject

    def transform_sample(self, sample, subject):
        """Transform sample."""
        _me = self

        def entity(self):
            s = Specimen.build_entity(self, subject)
            yield s
            outputs = []
            for blob in self.blobs.values():
                for b in _me.transform_blob(Blob(blob, sample)):
                    b = DocumentReference.build_entity(b, subject)
                    outputs.append(b)
                    yield b
            yield SpecimenTask.build_entity(inputs=[s], outputs=outputs)
        sample.entity = types.MethodType(entity, sample)
        yield sample

    def transform_blob(self, blob):
        """Transform blob (noop)."""
        def entity(self):
            yield DocumentReference.build_entity(self)
        blob.entity = types.MethodType(entity, blob)
        yield blob
