# set up a command library
import logging
from collections import defaultdict
from urllib.parse import urlparse

from attrdict import AttrDict

from anvil.etl.transformers import LogCapture
from anvil.etl.utilities.entities import Entities

# logger = LogCapture('anvil.etl_old.transformers.normalizer')
# logger.addHandler(logging.root.handlers[0])

# from contextvars import ContextVar
# logger = ContextVar('logger').get()
from . import logger


#
# runner
#
from ..extractors.gen3 import DRSReader


def exec_command(context, consortium_name, workspace, config, cmd):
    """Find and run version of the command that matches."""
    workspace_name = workspace['workspace']['name']

    paths = [
        f"{consortium_name}/{workspace_name}/{cmd}",
        f"{consortium_name}//{cmd}",
        f"//{workspace_name}/{cmd}",
        f"//{cmd}"
    ]

    path = None
    for path in paths:
        try:
            runner = C.get(path, None)
            if runner:
                return runner(context, consortium_name, workspace, config)
        except Exception as e:
            raise e

    assert False, f"Misconfiguration, we shouldn't get here. {cmd} {path} {consortium_name} {workspace_name}"


def _extract_bucket_fields(output_path=None, entities=None, workspace_name=None):
    """Query db, sample first row in all entities, determine fields that have references to bucket object."""
    if not entities:
        assert output_path
        entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    for workspace in entities.get_by_label('workspace'):
        workspace = AttrDict(workspace)
        if workspace_name and workspace.workspace.name != workspace_name:
            continue
        schema = AttrDict(entities.get_edges_by_label(src=workspace.workspace.name, src_name='workspace', dst_name='schema'))
        for entity_name in schema.schema.keys():
            if entity_name == 'schema':
                continue
            child = entities.get_edges_by_label(src=workspace.workspace.name, src_name='workspace', dst_name=entity_name, limit=1)
            if entity_name not in child:
                logger.error(('invalid.child', workspace.consortium_name, workspace.workspace.name, entity_name, child))
                continue
            bucket_keys = [k for k, v in child[entity_name]['attributes'].items() if isinstance(v, str) and v.startswith('gs://')]
            if len(bucket_keys) == 0:
                continue
            buckets = [child[entity_name]['attributes'][key] for key in bucket_keys]
            buckets = list(set([urlparse(b).netloc for b in buckets]))
            # TODO - move this exception to config or other ?
            if entity_name == 'sequencing' and 'AnVIL_CMG_Broad' in workspace.workspace.name:
                bucket_keys = 'crai_or_bai_path,crai_path,cram_or_bam_path,cram_path,md5_path'.split(',')
            yield {
                'consortium_name': workspace.consortium_name,
                'workspace_name': workspace.workspace.name,
                'entity_name': entity_name,
                'bucket_fields': bucket_keys,
                'buckets': buckets
            }


def _recursive_default_dict():
    """Recursive default dict, any key defaults to a dict."""
    return defaultdict(_recursive_default_dict)


def consortium_config(context, consortium_name, workspace, config):
    context.consortium_config = config[consortium_name]
    inverted_config = _recursive_default_dict()
    for fhir_entity in context.consortium_config['entities']:
        for alias in context.consortium_config['entities'][fhir_entity]['aliases']:
            inverted_config[alias] = fhir_entity
    context.inverted_config = inverted_config


def patient_entity_names(context, consortium_name, workspace, config):
    context.patient_entity_names = context.consortium_config['entities']['Patient']['aliases']


def patient_entity_names_CMG(context, consortium_name, workspace, config):
    patient_entity_names(context, consortium_name, workspace, config)


def patient_entity_names_CCDG(context, consortium_name, workspace, config):
    patient_entity_names(context, consortium_name, workspace, config)


def specimen_entity_name(context, consortium_name, workspace, config):
    entities_with_bucket_fields = [bf['entity_name'] for bf in context['bucket_fields']]
    specimen_aliases_with_buckets = []
    for alias in context.consortium_config['entities']['Specimen']['aliases']:
        if alias in entities_with_bucket_fields:
            specimen_aliases_with_buckets.append(alias)

    if len(specimen_aliases_with_buckets) == 0:
        context.specimen_entity_name = context.consortium_config['entities']['Specimen']['aliases'][0]
        logger.warning(('specimen.entity.has.no.bucket.fields', workspace.workspace.name, context.specimen_entity_name))
    elif 'sample' in specimen_aliases_with_buckets:
        context.specimen_entity_name = 'sample'
    else:
        context.specimen_entity_name = specimen_aliases_with_buckets[0]


def patient_from_specimen_numbered(context, consortium_name, workspace, config):
    """Look for workspace.specimen.01-subject_id -> workspace.subject."""

    return context.specimen['attributes']['01-subject_id']


def patient_from_specimen_subject_id(context, consortium_name, workspace, config):
    """Look for workspace.specimen.subject_id -> workspace.subject."""

    return context.specimen['attributes']['subject_id']


def patient_from_specimen_participant_id(context, consortium_name, workspace, config):
    """Look for context.specimen.participant_id -> workspace.subject."""

    return context.specimen['attributes']['participant_id']


def patient_from_specimen_subject(context, consortium_name, workspace, config):
    """Look for context.specimen.subject -> workspace.subject."""

    return context.specimen['attributes']['subject']


def patient_from_participant(context, consortium_name, workspace, config):
    """Look for context.specimen.subject -> workspace.subject."""

    return context.specimen['attributes']['participant']


def patient_from_specimen(context, consortium_name, workspace, config):
    """Introspect specimen, see if it has a dict pointing to subject."""

    patient_entity_name = None
    for n in context.patient_entity_names:
        if n in context.specimen['attributes']:
            patient_entity_name = n
            break

    if not patient_entity_name:
        if 'no.find.patient_entity_name' not in context['logged_already']:
            logger.error(('no.find.patient_entity_name', consortium_name, workspace.workspace.name,
                          context.patient_entity_names, list(workspace.children.keys())))  # list since cannot pickle 'dict_keys' object
            context['logged_already'].append('no.find.patient_entity_name')
        return None

    if patient_entity_name not in context.specimen['attributes']:
        if 'specimen.missing.patient' not in context['logged_already']:
            logger.error(('specimen.missing.patient', consortium_name, workspace.workspace.name, context.specimen))
            context['logged_already'].append('specimen.missing.patient')
        return None

    patient_key_from_specimen = context.specimen['attributes'][patient_entity_name]
    if isinstance(patient_key_from_specimen, dict):
        patient_key_from_specimen = patient_key_from_specimen['entityName']
    return patient_key_from_specimen


def patient_from_participant_pfb_subject(context, consortium_name, workspace, config):
    """Introspect specimen, see if it has a dict pointing to subject."""

    patient_entity_name = None
    for n in context.patient_entity_names:
        if n in context.specimen['attributes']:
            patient_entity_name = n
            break

    if not patient_entity_name:
        if 'no.find.patient_entity_name' not in context['logged_already']:
            logger.error(('no.find.patient_entity_name', consortium_name, workspace.workspace.name))
            context['logged_already'].append('no.find.patient_entity_name')
        return None

    if patient_entity_name not in context.specimen['attributes']:
        if 'specimen.missing.patient' not in context['logged_already']:
            logger.error(('specimen.missing.patient', consortium_name, workspace.workspace.name, context.specimen))
            context['logged_already'].append('specimen.missing.patient')
        return None

    patient_key_from_specimen = context.specimen['attributes'][patient_entity_name]
    if isinstance(patient_key_from_specimen, dict):
        patient_key_from_specimen = f"pfb:{patient_key_from_specimen['entityName']}"
    return patient_key_from_specimen


def patient_from_qc_result_sample(context, consortium_name, workspace, config):
    """Introspect specimen, see if it has a dict pointing to subject."""
    sample_id = context.specimen['attributes']['qc_result_sample']
    for s in workspace.children['sample']:
        if s['name'] == sample_id or s['attributes'].get('collaborator_sample_id', None) == sample_id:
            for k in ['subject_id', 'participant_id', 'collaborator_participant_id']:
                if k in s['attributes']:
                    return s['attributes'][k]
            logger.error(('no.subject.found', workspace.workspace.name, sample_id, s))


def patient_from_specimen_all(context, consortium_name, workspace, config):
    """Try all commands."""
    cmd = None
    if 'subject' in context.specimen['attributes']:
        cmd = patient_from_specimen_subject
    if 'participant_id' in context.specimen['attributes']:
        cmd = patient_from_specimen_participant_id
    if '01-subject_id' in context.specimen['attributes']:
        cmd = patient_from_specimen_numbered
    if 'subject_id' in context.specimen['attributes']:
        cmd = patient_from_specimen_subject_id
    if 'specimen' in context.specimen['attributes']:
        cmd = patient_from_specimen
    if 'participant' in context.specimen['attributes']:
        cmd = patient_from_participant
    if 'pfb:subject' in context.specimen['attributes']:
        cmd = patient_from_participant_pfb_subject
    if 'qc_result_sample' in context.specimen['attributes']:
        cmd = patient_from_qc_result_sample

    if cmd:
        return cmd(context, consortium_name, workspace, config)
    else:
        if 'unparsable.specimen' not in context['logged_already']:
            logger.error(('unparsable.specimen', consortium_name, workspace.workspace.name, context.specimen))
            context['logged_already'].append('unparsable.specimen')
        return None


def task_entity_names(context, consortium_name, workspace, config):
    """Set task_entity_names on context to aliases, or _implied if no Task configured."""
    if 'Task' in context.consortium_config['entities']:
        context.task_entity_names = context.consortium_config['entities']['Task']['aliases']
    else:
        context.task_entity_names = ['_implied']


def ensure_tasks(context, consortium_name, workspace, config):
    """Set tasks on workspace to a workspace child, or create an artificial one if _implied."""
    workspace['tasks'] = {}
    if '_implied' in context.task_entity_names:
        workspace['tasks']['_implied'] = _recursive_default_dict()
    else:
        for task_entity_name in context.task_entity_names:
            if task_entity_name in workspace.children.keys():
                workspace['tasks'][task_entity_name] = workspace.children[task_entity_name]
        if not workspace['tasks']:
            workspace['tasks']['_implied'] = _recursive_default_dict()


def ensure_bucket_fields(context, consortium_name, workspace, config):
    """Introspect workspace schema looking for fields that have bucket objects."""

    context['bucket_fields'] = [bf for bf in
                                _extract_bucket_fields(entities=context['entities'], workspace_name=workspace.workspace.name)]


def ensure_bucket_fieldsAnVIL_CMG_BaylorHopkins_HMB_NPU_WES(context, consortium_name, workspace, config):
    """Override sample.bucket_fields"""
    ensure_bucket_fields(context, consortium_name, workspace, config)
    assert len(context['bucket_fields']) == 1
    context['bucket_fields'][0]['bucket_fields'] = "bam,bam_md5,crai,cram,cram_md5".split(',')


def ensure_bucket_fieldsAnVIL_CMG_Broad_crai_or_bai(context, consortium_name, workspace, config):
    """Override sample.bucket_fields"""
    ensure_bucket_fields(context, consortium_name, workspace, config)
    assert len(context['bucket_fields']) == 1
    context['bucket_fields'][0][
        'bucket_fields'] = 'crai_or_bai_path,crai_path,cram_or_bam_path,cram_path,md5_path'.split(',')


def ensure_tasks_populated(context, consortium_name, workspace, config):
    """Create a terra style entity for an implied task if none exists."""
    if '_implied' in workspace['tasks']:
        assert workspace.specimens
        workspace['tasks']['_implied'] = []
        for specimen in workspace.specimens:
            # create terra type entity
            workspace['tasks']['_implied'].append(AttrDict({
                'entityType': 'Task',
                'name': specimen['name'],
                'attributes': {
                    # TODO - are there any imputed attributes?
                }
            }))

    # check that other keys exist
    assert len(workspace['tasks']) > 0, ('missing.tasks', consortium_name, workspace.workspace.name)


def extract_specimen_reference_sample_alias(context, consortium_name, workspace, config):
    """Extract a specimen key from arbitrary entity, reads from ctx.entity_with_specimen_reference, sets specimen_reference aka the key."""
    assert 'entity_with_specimen_reference' in context
    entity = context['entity_with_specimen_reference']
    if 'sample_alias' in entity['attributes']:
        context['specimen_reference'] = {
            'name': entity['attributes']['sample_alias'],
            'entityType': 'sample',
            'fhir_entity': context.inverted_config['sample']
        }


def extract_specimen_reference_sample_id(context, consortium_name, workspace, config):
    """Extract a specimen key from arbitrary entity, reads from ctx.entity_with_specimen_reference, sets specimen_reference aka the key."""
    assert 'entity_with_specimen_reference' in context
    entity = context['entity_with_specimen_reference']
    if 'sample_id' in entity['attributes']:
        context['specimen_reference'] = {
            'name': entity['attributes']['sample_id'],
            'entityType': 'sample',
            'fhir_entity': context.inverted_config['sample']
        }


def extract_specimen_reference_collaborator_sample_id(context, consortium_name, workspace, config):
    """Extract a specimen key from arbitrary entity, reads from ctx.entity_with_specimen_reference, sets specimen_reference aka the key."""
    assert 'entity_with_specimen_reference' in context
    entity = context['entity_with_specimen_reference']
    if 'collaborator_sample_id' in entity['attributes']:
        context['specimen_reference'] = {
            'name': entity['attributes']['collaborator_sample_id'],
            'entityType': 'sample',
            'fhir_entity': context.inverted_config['sample']
        }


def extract_specimen_reference_CMG(context, consortium_name, workspace, config):
    """Extract a specimen key from arbitrary entity, reads from ctx.entity_with_specimen_reference, sets specimen_reference aka the key."""
    assert 'entity_with_specimen_reference' in context
    entity = context['entity_with_specimen_reference']
    for key in ['collaborator_sample_id', 'sample', 'sample_alias']:
        if key in entity['attributes']:
            context['specimen_reference'] = {
                'name': entity['attributes'][key],
                'entityType': 'sample',
                'fhir_entity': context.inverted_config['sample']
            }


def ensure_tasks_linked_to_documents(context, consortium_name, workspace, config):
    """Ensure tasks have input that is a specimen, and outputs that are document references."""

    # set up a hash of children with buckets for quick lookup
    _children = _recursive_default_dict()
    for bf in context.bucket_fields:
        if bf['entity_name'] in workspace.children:
            for child in workspace.children[bf['entity_name']]:
                _children[bf['entity_name']][child['name']] = child
    # create a task, whose inputs are entities with bucket fields.
    # and whose outputs are those bucket fields
    deletion_keys = []
    for task_entity in workspace['tasks']:
        document_count = 0
        for task in workspace['tasks'][task_entity]:
            _task_children = _recursive_default_dict()
            _task_inputs = []
            for bf in context.bucket_fields:
                if bf['entity_name'] in _children:
                    if task['name'] in _children[bf['entity_name']]:
                        child = _children[bf['entity_name']][task['name']]
                        _task_inputs.append(
                            {
                                'entityType': child['entityType'],
                                'name': child['name'],
                                'fhir_entity': context.inverted_config[child['entityType']]
                            }
                        )
                        for field in bf['bucket_fields']:
                            if workspace.workspace.name not in ['AnVIL_CMG_BaylorHopkins_HMB-NPU_WES']:
                                # TODO - remove if not needed
                                if 'AnVIL_CMG_Broad' not in workspace.workspace.name:
                                    assert field in child['attributes'], (field, child, bf)
                            if field in child['attributes']:
                                _task_children[bf['entity_name']][field] = child['attributes'][field]
                                document_count += 1
                # # a task might have it's own bucket fields
                # for field in bf['bucket_fields']:
                #     logger.error(('?', field))
                #     if field in task['attributes']:
                #         logger.error(('?', bf['entity_name'], field))
                #         _task_children[bf['entity_name']][field] = task['attributes'][field]
                #         document_count += 1

            if 'Specimen' not in [ti['fhir_entity'] for ti in _task_inputs]:
                no_specimen = True
                for ti in _task_inputs:
                    context.entity_with_specimen_reference = _children[ti['entityType']][ti['name']]
                    exec_command(context, consortium_name, workspace, config, 'extract_specimen_reference')
                    if 'specimen_reference' in context:
                        _task_inputs.append(context['specimen_reference'])
                        no_specimen = False
                        del context['specimen_reference']
                if no_specimen and len(_task_inputs) > 0:
                    logger.error(('no.specimen.in.task', consortium_name, workspace.workspace.name, _task_inputs))

            # It is possible for an entity that has  bucket fields to be classified as a task,
            # make sure we don't self reference
            _task_inputs = [ti for ti in _task_inputs if
                            not (task['name'] == ti['name'] and task['entityType'] == ti['entityType'])]

            task['outputs'] = _task_children
            task['inputs'] = _task_inputs
            if document_count == 0:
                deletion_keys.append(task_entity)

    # delete task entities without documents
    for key in set(deletion_keys):
        del workspace['tasks'][key]
        logger.warning(('no.task', consortium_name, workspace.workspace.name, key))


def blob_attributes(context, consortium_name, workspace, config):
    """Decorate task outputs with blob attributes."""
    google_entities = None
    if context['validate_buckets']:
        google_entities = Entities(path=f"{context['output_path']}/google_entities.sqlite")
    drs_reader = DRSReader(context['output_path'])

    missing_blob_count = 0
    for task_entity in workspace['tasks']:
        for task in workspace['tasks'][task_entity]:
            for output_source, output in task['outputs'].items():
                for output_property, url in output.items():
                    if context['validate_buckets']:
                        blob = google_entities.get(url)
                        if not blob:
                            blob = {'url': url, 'drs_uri': None}
                            missing_blob_count += 1
                            if 'blob.not.in.bucket' not in context['logged_already']:
                                logger.warning(('blob.not.in.bucket', consortium_name, workspace.workspace.name,
                                                output_source, output_property, url))
                                context['logged_already'].append('blob.not.in.bucket')
                    else:
                        blob = {'url': url, 'drs_uri': None}
                    drs = drs_reader.get(url.split('/')[-1])
                    if drs:
                        blob['drs_uri'] = drs.get('ga4gh_drs_uri', None)
                    task['outputs'][output_source][output_property] = blob
    if missing_blob_count > 0:
        logger.error(('total.blob.not.in.bucket', consortium_name, workspace.workspace.name, missing_blob_count))


def practitioner(context, consortium_name, workspace, config):
    """Determine the PI, set practitioner on workspace."""
    if 'study_pi' in workspace.workspace.attributes:
        workspace.practitioner = workspace.workspace.attributes['study_pi']
    elif 'library:institute' in workspace.workspace.attributes:
        workspace.practitioner = workspace.workspace.attributes['library:institute']['items'][0]
    else:
        logger.error(
            ('no.find.practitioner', consortium_name, workspace.workspace.name, workspace.workspace.attributes))
        workspace.practitioner = 'NA'
    if not workspace.practitioner or len(workspace.practitioner) == 0:
        workspace.practitioner = 'NA'


def phenotypes(context, consortium_name, workspace, config):
    """Retrieve phenotypes from patient."""
    context['patient']['phenotype_values'] = []


def phenotypes_fields_CMG(context, consortium_name, workspace, config):
    """Retrieve phenotypes from patient."""
    if 'phenotype_fields' not in context:
        schema = workspace.children['schema'][0]
        phenotype_fields = []
        for entity_name in schema.keys():
            for attribute_name in schema[entity_name]['attributeNames']:
                for k in ['pheno', 'hpo', 'genotype']:
                    if k in attribute_name:
                        phenotype_fields.append((entity_name, attribute_name))
        context['phenotype_fields'] = phenotype_fields

    phenotype_values = []
    for key, value in context['patient']['attributes'].items():
        for entity_name, attribute_name in context['phenotype_fields']:
            if key == attribute_name:
                phenotype_values.append((key, value))
    context['patient']['phenotype_values'] = phenotype_values


def phenotypes_CMG(context, consortium_name, workspace, config):
    """Retrieve phenotypes from patient."""
    phenotypes_fields_CMG(context, consortium_name, workspace, config)
    phenotype_values = defaultdict(set)
    for attribute_name, value in context['patient']['phenotype_values']:
        if not value or value == '-':
            continue
        for k in ['hpo_present']:
            if k in attribute_name:
                phenotype_values['present'].update(value.split('|'))
        for k in ['phenotype_group']:
            if k in attribute_name:
                phenotype_values['group'].update(value.split('|'))
        for k in ['hpo_absent']:
            if k in attribute_name:
                phenotype_values['absent'].update(value.split('|'))
        for k in ['phenotype_description']:
            if k in attribute_name:
                phenotype_values['description'].update(value.split('|'))
    # xform to a list
    for k, v in phenotype_values.items():
        phenotype_values[k] = list(v)
    context['patient']['phenotype_values'] = [phenotype_values]


def phenotypes_CCDG(context, consortium_name, workspace, config):
    """Retrieve phenotypes from patient."""
    phenotypes(context, consortium_name, workspace, config)
    if len(context['patient']['phenotype_values']) > 0:
        logger.error(('TODO.phenotypes_CCDG', context['patient']['phenotype_values']))
    context['patient']['phenotype_values'] = []


def disease(context, consortium_name, workspace, config):
    """Retrieve disease from patient."""
    context['patient']['disease_values'] = []


def disease_CMG(context, consortium_name, workspace, config):
    """Retrieve disease from patient."""
    disease_values = defaultdict(set)
    for match in ['disease', 'affected']:
        for key, value in context['patient']['attributes'].items():
            if match in key:
                if value.lower() in ["n/a", 'na', 'nan', '-', '']:
                    continue
                values = [v for v in value.split('|') if len(v) > 0]
                disease_values[key].update(values)

    if 'affected_status' in disease_values and 'disease_description' in disease_values:
        disease_values['description'].update(
            disease_values['disease_description'])  # = list(disease_values['disease_description'])[0],
        disease_values['present'].update(
            disease_values['affected_status'])  # = list(disease_values['affected_status'])[0]

    context['patient']['disease_values'] = [disease_values]


def disease_NIMH(context, consortium_name, workspace, config):
    """Retrieve disease from patient."""
    disease_CMG(context, consortium_name, workspace, config)


def disease_CCDG(context, consortium_name, workspace, config):
    """Retrieve disease from patient."""
    disease_values = defaultdict(set)
    for attribute_name in ['ADI-R_DIAG', 'affected_status', 'affected_status_detail', 'affection_status',
                           'atrial_fibrillation', 'atrial_fibrilliation', 'crohns_disease', 'disease_description',
                           'epilepsy', 'hemorrhagic_stroke', 'ischemic_stroke', 'ulcerative_colitis']:
        for key, value in context['patient']['attributes'].items():
            if key == attribute_name:
                if value.lower() in ["n/a", 'na', 'nan']:
                    continue
                disease_values[key].add(value)

    if 'affected_status' in disease_values and 'disease_description' in disease_values:
        disease_values['description'].update(
            disease_values['disease_description'])  # = list(disease_values['disease_description'])[0],
        disease_values['present'].update(
            disease_values['affected_status'])  # = list(disease_values['affected_status'])[0]
    context['patient']['disease_values'] = [disease_values]


def disease_CCDG_ADI_R_DIAG(context, consortium_name, workspace, config):
    """Retrieve disease from patient. AnVIL_CCDG_NYGC_NP_Autism_ACE2_GRU-MDS_WGS"""
    disease_CCDG(context, consortium_name, workspace, config)
    disease_values_list = []

    disease_values = context['patient']['disease_values']
    if "ADI-R_DIAG" in disease_values:
        description = disease_values["ADI-R_DIAG"]
        present = disease_values['affection_status']
        disease_values_list.append({
            'description': description,
            'present': present
        })

    for k in ["atrial_fibrilliation", "crohns_disease", "epilepsy", "hemorrhagic_stroke", "ulcerative_colitis"]:
        if k in disease_values:
            disease_values_list.append({
                'description': k.replace('_', ' '),
                'present': disease_values[k]
            })

    context['patient']['disease_values'] = disease_values_list


def gender(context, consortium_name, workspace, config):
    """Retrieve gender from patient."""
    context['patient']['gender'] = None


def gender_NHGRI(context, consortium_name, workspace, config):
    """Retrieve gender from patient."""
    assert 'patient' in context
    keys = ["gender"]
    for k in keys:
        if k in context['patient']['attributes']:
            context['patient']['gender'] = context['patient']['attributes'][k].lower()
            assert context['patient']['gender'] in "male,female,other,unknown".split(',')


def gender_NIMH(context, consortium_name, workspace, config):
    """Retrieve gender from patient."""
    gender_CMG(context, consortium_name, workspace, config)


def gender_CMG(context, consortium_name, workspace, config):
    """Retrieve gender from patient."""
    assert 'patient' in context
    keys = ["sex"]
    context['patient']['gender'] = None
    for k in keys:
        if k in context['patient']['attributes']:
            value = context['patient']['attributes'][k].lower()
            if value in ['-', 'not reported', 'notreported', 'na', 'unspecified']:
                value = 'unknown'
            if value not in "male,female,other,unknown".split(','):
                logger.error(('unrecognized.gender', value))
                value = 'unknown'
            context['patient']['gender'] = value
            assert context['patient']['gender'] in "male,female,other,unknown".split(','), context['patient']['gender']


def gender_CCDG(context, consortium_name, workspace, config):
    """Retrieve gender from patient."""
    gender_CMG(context, consortium_name, workspace, config)


def family_relationship(context, consortium_name, workspace, config):
    """Retrieve family relationship from patient."""
    family_relationship_values = {}
    for match in ['family_id', 'proband_relationship', 'family_relationship', 'maternal', 'paternal', 'father',
                  'mother']:
        for key, value in context['patient']['attributes'].items():
            if match in key.lower():
                if value.lower() in ["#n/a", "n/a", 'na', 'nan', '-', '']:
                    continue
                values = [v for v in value.split('|') if len(v) > 0]
                assert len(values) == 1, values
                family_relationship_values[match] = values[0]

    context['patient']['family_relationship_values'] = [family_relationship_values]

    if len(family_relationship_values) > 0:
        assert 'FamilyRelationship' in context.consortium_config['entities']
        if 'FamilyRelationship' in context.consortium_config['entities']:
            for alias in context.consortium_config['entities']['FamilyRelationship']['aliases']:
                if alias in workspace['children']:
                    for family in workspace['children'][alias]:
                        if family['name'] == family_relationship_values['family_id']:
                            context['patient']['family'] = family
            if 'family' not in context['patient']:
                logger.error(('no.family', consortium_name, workspace.workspace.name, context['patient']['name']))

# def family_relationship_CMG(context, consortium_name, workspace, config):
#     """Retrieve family relationship from patient."""
#     ["family_id", "family_relationship", "maternal_id", "paternal_id"]
#     pass

def body_site(context, consortium_name, workspace, config):
    """Retrieve body site from specimen."""
    pass


def body_site_CMG(context, consortium_name, workspace, config):
    """Retrieve body site from specimen."""
    # TODO ["sample_source"]  
    pass


def link_specimen_to_patient(context, consortium_name, workspace, config):
    """Standard model specimen->patient."""

    # start traversal from specimen
    specimens = workspace.children.get(context.specimen_entity_name, None)
    if not specimens or len(specimens) == 0:
        logger.error(('no.find.specimens', consortium_name, workspace.workspace.name))
        return
    workspace.specimens = specimens

    # traverse up to patient
    logged_already = False
    specimen_patient_ids = []
    missing_patient_count = 0
    for specimen in workspace.specimens:
        context.specimen = specimen
        patient_id = exec_command(context, consortium_name, workspace, config, 'patient_from_specimen')
        if not patient_id:
            if not logged_already:
                logged_already = True
                logger.error((
                             "no.find.patient.from.sample", consortium_name, workspace.workspace.name, context.specimen,
                             list(workspace.children.keys())))
            missing_patient_count += 1
            continue
        else:
            specimen_patient_ids.append((specimen.name, patient_id))
    if missing_patient_count:
        logger.error(
            ("total.no.find.patient.from.sample", consortium_name, workspace.workspace.name, missing_patient_count))
    # store sample->patient on workspace for now
    total_patient_count = 0
    for patient_entity_name in context.patient_entity_names:
        # patient_entity_names are possibilities, they don't have to exist
        total_patient_count += len(workspace.children.get(patient_entity_name, []))
    workspace.specimen_patient_ids = specimen_patient_ids
    workspace.total_patient_count = total_patient_count


def patient_model(context, consortium_name, workspace, config):
    """Standard model patient->specimen->task->document_reference."""
    patients = {}
    specimens = {}
    if 'patient_entity_names' not in context:
        logger.warning(('missing.context.patient_entity_names', consortium_name, workspace.workspace.name))
        return
    for patient_entity_name in context.patient_entity_names:
        if patient_entity_name in workspace.children:
            for p in workspace.children[patient_entity_name]:
                # normalize patient properties
                context['patient'] = p
                exec_command(context, consortium_name, workspace, config, 'phenotypes')
                exec_command(context, consortium_name, workspace, config, 'disease')
                exec_command(context, consortium_name, workspace, config, 'gender')
                exec_command(context, consortium_name, workspace, config, 'family_relationship')
                # hash by several keys, the name and any other we know about
                patients[p['name']] = p
                for k in ['collaborator_participant_id']:
                    if k in p['attributes']:
                        patients[p['attributes'][k]] = p

    if len(patients) == 0:
        logger.error(('no.patients.found', consortium_name, workspace.workspace.name,
                      f"{context.patient_entity_names} not found in {workspace.children.keys()}"))
        return

    assert context.specimen_entity_name in workspace.children.keys()
    for s in workspace.children[context.specimen_entity_name]:
        # normalize specimen properties
        context['specimen'] = s
        exec_command(context, consortium_name, workspace, config, 'body_site')
        # hash by several keys, the name and any other we know about
        specimens[s['name']] = s
        for k in ['collaborator_sample_id']:
            if k in s['attributes']:
                specimens[s['attributes'][k]] = s

    for task_entity in workspace.tasks:
        for task in workspace.tasks[task_entity]:
            for input in task['inputs']:
                if input['fhir_entity'] == 'Specimen':
                    if input['name'] not in specimens:
                        logger.error(('no.specimen.found.for.task', consortium_name, workspace.workspace.name,
                                      task['entityType'], input['name']))
                        continue
                    if 'tasks' not in specimens[input['name']]:
                        specimens[input['name']]['tasks'] = []
                    specimens[input['name']]['tasks'].append(task)

    missing_patient_count = 0
    for specimen_id, patient_id in workspace.specimen_patient_ids:
        # assert patient_id, (specimen_id, patient_id)
        if patient_id and not isinstance(patient_id, str):
            # hmmm - should we always just make this a str
            patient_id = patient_id['entityName']
        if patient_id not in patients:
            missing_patient_count += 1
            if "no.find.patient.specified.by.specimen" not in context.logged_already:
                logger.error(("no.find.patient.specified.by.specimen", consortium_name, workspace.workspace.name,
                              context.specimen_entity_name, patient_id))
                context['logged_already'].append("no.find.patient.specified.by.specimen")
            continue
        if specimen_id not in specimens:
            logger.error(("no.find.specimen", consortium_name, workspace.workspace.name, specimen_id))
            continue
        if 'specimens' not in patients[patient_id]:
            patients[patient_id]['specimens'] = []
        patients[patient_id]['specimens'].append(specimens[specimen_id])
    if missing_patient_count:
        logger.error(("total.no.find.patient.specified.by.specimen", consortium_name, workspace.workspace.name,
                      missing_patient_count))
    # set on workspace
    workspace.patients = patients


#
# command specializations hierarchy
#
C = {}
C['//consortium_config'] = lambda context, consortium_name, workspace, config: consortium_config(context, consortium_name, workspace, config)
C['CMG//patient_entity_names'] = lambda context, consortium_name, workspace, config: patient_entity_names_CMG(context, consortium_name, workspace, config)
C['CCDG//patient_entity_names'] = lambda context, consortium_name, workspace, config: patient_entity_names_CCDG(context, consortium_name, workspace, config)
C['//patient_entity_names'] = lambda context, consortium_name, workspace, config: patient_entity_names(context, consortium_name, workspace, config)
C['//specimen_entity_name'] = lambda context, consortium_name, workspace, config: specimen_entity_name(context, consortium_name, workspace, config)
C['//patient_from_specimen'] = lambda context, consortium_name, workspace, config: patient_from_specimen_all(context, consortium_name, workspace, config)
C['//task_entity_names'] = lambda context, consortium_name, workspace, config: task_entity_names(context, consortium_name, workspace, config)
C['//ensure_tasks'] = lambda context, consortium_name, workspace, config: ensure_tasks(context, consortium_name, workspace, config)
C['//ensure_tasks_populated'] = lambda context, consortium_name, workspace, config: ensure_tasks_populated(context, consortium_name, workspace, config)
C['//ensure_tasks_linked_to_documents'] = lambda context, consortium_name, workspace, config: ensure_tasks_linked_to_documents(context, consortium_name, workspace, config)
C['//ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fields(context, consortium_name, workspace, config)
C['CMG/AnVIL_CMG_BaylorHopkins_HMB-NPU_WES/ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fieldsAnVIL_CMG_BaylorHopkins_HMB_NPU_WES(context, consortium_name, workspace, config)
C['CMG/AnVIL_CMG_Broad_Kidney_Hildebrandt_WES/ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fieldsAnVIL_CMG_Broad_crai_or_bai(context, consortium_name, workspace, config)
C['CMG/AnVIL_CMG_Broad_Muscle_KNC_WES/ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fieldsAnVIL_CMG_Broad_crai_or_bai(context, consortium_name, workspace, config)
C['//blob_attributes'] = lambda context, consortium_name, workspace, config: blob_attributes(context, consortium_name, workspace, config)

C['CCDG//extract_specimen_reference'] = lambda context, consortium_name, workspace, config: extract_specimen_reference_sample_id(context, consortium_name, workspace, config)
C['CMG//extract_specimen_reference'] = lambda context, consortium_name, workspace, config: extract_specimen_reference_CMG(context, consortium_name, workspace, config)
C['NIMH//extract_specimen_reference'] = lambda context, consortium_name, workspace, config: extract_specimen_reference_sample_id(context, consortium_name, workspace, config)

C['//practitioner'] = lambda context, consortium_name, workspace, config: practitioner(context, consortium_name, workspace, config)

C['//phenotypes'] = lambda context, consortium_name, workspace, config: phenotypes(context, consortium_name, workspace, config)
C['CMG//phenotypes'] = lambda context, consortium_name, workspace, config: phenotypes_CMG(context, consortium_name, workspace, config)
C['CCDG//phenotypes'] = lambda context, consortium_name, workspace, config: phenotypes_CCDG(context, consortium_name, workspace, config)

C['//disease'] = lambda context, consortium_name, workspace, config: disease(context, consortium_name, workspace, config)
C['CMG//disease'] = lambda context, consortium_name, workspace, config: disease_CMG(context, consortium_name, workspace, config)
C['CCDG//disease'] = lambda context, consortium_name, workspace, config: disease_CCDG(context, consortium_name, workspace, config)
C['NIMH//disease'] = lambda context, consortium_name, workspace, config: disease_NIMH(context, consortium_name, workspace, config)
C['NHGRI//disease'] = lambda context, consortium_name, workspace, config: disease_NIMH(context, consortium_name, workspace, config)

C['CCDG/AnVIL_CCDG_NYGC_NP_Autism_ACE2_GRU-MDS_WGS/disease'] = lambda context, consortium_name, workspace, config: disease_CCDG_ADI_R_DIAG(context, consortium_name, workspace, config)

C['//gender'] = lambda context, consortium_name, workspace, config: gender(context, consortium_name, workspace, config)
C['CMG//gender'] = lambda context, consortium_name, workspace, config: gender_CMG(context, consortium_name, workspace, config)
C['NHGRI//gender'] = lambda context, consortium_name, workspace, config: gender_NHGRI(context, consortium_name, workspace, config)
C['NIMH//gender'] = lambda context, consortium_name, workspace, config: gender_NIMH(context, consortium_name, workspace, config)
C['CCDG//gender'] = lambda context, consortium_name, workspace, config: gender_CCDG(context, consortium_name, workspace, config)

C['//family_relationship'] = lambda context, consortium_name, workspace, config: family_relationship(context, consortium_name, workspace, config)

C['//body_site'] = lambda context, consortium_name, workspace, config: body_site(context, consortium_name, workspace, config)
C['CMG//body_site'] = lambda context, consortium_name, workspace, config: body_site_CMG(context, consortium_name, workspace, config)

C['//link_specimen_to_patient'] = lambda context, consortium_name, workspace, config: link_specimen_to_patient(context, consortium_name, workspace, config)

C['//patient_model'] = lambda context, consortium_name, workspace, config: patient_model(context, consortium_name, workspace, config)
