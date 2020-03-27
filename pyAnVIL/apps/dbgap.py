import logging
import json
from dbgap.fetch import fetch_schemas
from dbgap.known_projects import dbgap_projects
from pprint import pprint
import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv


def main():
    projects = None

    # load the AnVIL->dbGAP project mappings
    if not os.path.exists('dbGAP_projects.json'):
        projects = dbgap_projects()
        with open('dbGAP_projects.json', 'w') as fp:
            json.dump(projects, fp)
    else:
        with open('dbGAP_projects.json', 'r') as fp:
            projects = json.load(fp)
    project_ids = projects.keys()

    # get the dbGAP schemas
    if not os.path.exists('dbGAP_schemas.json'):
        schemas, project_ids_with_errors = fetch_schemas(project_ids)
        # print('-------------  dbGAP no finds')
        # pprint({p: projects[p] for p in project_ids_with_errors})
        with open('dbGAP_schemas.json', 'w') as fp:
            json.dump(schemas, fp)
    else:
        with open('dbGAP_schemas.json', 'r') as fp:
            schemas = json.load(fp)
            project_ids_with_errors = []

    # import pdb; pdb.set_trace()

    # print('-------------  dbGAP find')
    # pprint({p: projects[p] for p in list(set(project_ids) - set(project_ids_with_errors))})
    #
    # print('-------------  dbGAP problems')
    # pprint({p: schemas[p]['errors'] for p in schemas if len(schemas[p]['errors']) > 0})

    # reverse lookup AnVIL.project_id -> dbgap_id
    dbgap_lookup = {}
    for dbgap_id in list(set(project_ids) - set(project_ids_with_errors)):
        for project_id in projects[dbgap_id]:
            dbgap_lookup[project_id] = dbgap_id

    # get data dashboard
    with open('notebooks/figures/report-data.json', 'r') as fp:
        report_data = json.load(fp)
    # add the dbGAP project id
    for project in report_data['projects']:
        project_id = project['project_id']
        if project_id in dbgap_lookup:
            project['dbGAP_project_id'] = dbgap_lookup[project_id]

    subject_counts = []
    for project in report_data['projects']:
        project_id = project['project_id']
        s = {'project_id': project_id, 'dbGAP_project_id': None}
        # get the terra Subject count
        try:
            subject_count = [n['count'] for n in project['nodes'] if n['type'] == 'Subject'][0]
            sample_count = [n['count'] for n in project['nodes'] if n['type'] == 'Sample'][0]
        except Exception:
            print(f"{project_id} has no Subject/Sample")
            pprint(project['nodes'])
            continue

        if project_id in dbgap_lookup:
            found = False
            s['dbGAP_project_id'] = dbgap_lookup[project_id]
            print(project_id, s['dbGAP_project_id'], project['dbGAP_project_id'])
            for k in schemas[project['dbGAP_project_id']]['counts'].keys():
                # get Subject count
                if k.endswith('Subject'):
                    found = True
                    s['dbGAP_subject_property'] = k
                    s['dbGAP_subject_property_path'] = schemas[project['dbGAP_project_id']]['counts'][k]['var_report_path']
                    s['terra_subject_count'] = subject_count
                    var_report = schemas[project['dbGAP_project_id']]['counts'][k]['var_report']
                    for v in var_report:
                        if v.startswith('SUB') and v.endswith('ID'):
                            count = 0
                            for n in var_report[v]['stats']['stat']:
                                count += int(var_report[v]['stats']['stat'][n])
                            s['dbGAP_subject_count'] = count
                # get Sample count
                if k.endswith('Sample'):
                    found = True
                    s['dbGAP_sample_property'] = k
                    s['dbGAP_sample_property_path'] = schemas[project['dbGAP_project_id']]['counts'][k]['var_report_path']
                    s['terra_sample_count'] = sample_count
                    var_report = schemas[project['dbGAP_project_id']]['counts'][k]['var_report']
                    for v in var_report:
                        if v.startswith('SAM') and v.endswith('ID'):
                            count = 0
                            for n in var_report[v]['stats']['stat']:
                                count += int(var_report[v]['stats']['stat'][n])
                            s['dbGAP_sample_count'] = count
            if not found:
                # print('\t', schemas[project['dbGAP_project_id']])
                s['schema'] = schemas[project['dbGAP_project_id']]
        subject_counts.append(s)

    keys = ['project_id', 'dbGAP_project_id', 'dbGAP_subject_property', 'dbGAP_subject_property_path', 'terra_subject_count', 'dbGAP_subject_count', 'dbGAP_sample_property', 'dbGAP_sample_property_path', 'terra_sample_count', 'dbGAP_sample_count', 'schema']
    with open('notebooks/figures/report-dbGAP.tsv', 'w') as fp:
        fp.write('\t'.join(keys))
        fp.write('\n')
        for s in subject_counts:
            for k in keys:
                v = s.get(k, None)
                if not v:
                    v = ''
                fp.write(f"{v}\t")
            fp.write('\n')

    with open('notebooks/figures/report-data.json', 'w') as fp:
        json.dump(report_data, fp)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    main()
