import logging
import json

from dbgap.studies import get_sample_count
from dbgap.known_projects import dbgap_projects

from pprint import pprint
import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv


def main():
    dbGap_projects = None

    # load the AnVIL->dbGAP project mappings
    dbGap_projects = dbgap_projects()
    # make some room for other info
    for study_id in dbGap_projects:
        terra_projects = dbGap_projects[study_id]
        dbGap_projects[study_id] = {'terra_projects': terra_projects}

    # add the info retrieved from dbGAP    
    study_ids = dbGap_projects.keys()
    sample_counts = map(lambda study_id: get_sample_count(study_id), study_ids)
    
    # add it to project counts, and create a reverse lookup to terra
    for study_id, sample_count in zip(study_ids, sample_counts):
        if sample_count:
            dbGap_projects[study_id]['qualified_study_id'] = sample_count[0]
            dbGap_projects[study_id]['sample_count'] = sample_count[1]
        else:
            dbGap_projects[study_id]['qualified_study_id'] = None
            dbGap_projects[study_id]['sample_count'] = None
    
    terra_to_dbGap = {}
    for study_id in dbGap_projects:
        for project_id in dbGap_projects[study_id]['terra_projects']:
            terra_to_dbGap[project_id] = {}
            terra_to_dbGap[project_id]['study_id'] = study_id
            terra_to_dbGap[project_id]['qualified_study_id'] = dbGap_projects[study_id]['qualified_study_id']
            terra_to_dbGap[project_id]['sample_count'] = dbGap_projects[study_id]['sample_count']


    # get data dashboard
    with open('notebooks/figures/report-data.json', 'r') as fp:
        report_data = json.load(fp)
    # add the dbGAP project id, and sample_counts
    for project in report_data['projects']:
        project_id = project['project_id']
        if project_id in terra_to_dbGap:
            project['dbGAP_study_id'] = terra_to_dbGap[project_id]['study_id']
            project['dbGAP_acession'] = terra_to_dbGap[project_id]['qualified_study_id']
            project['dbGAP_sample_count'] = terra_to_dbGap[project_id]['sample_count']
        else:
            project['dbGAP_acession'] = None
            project['dbGAP_sample_count'] = None
            if project_id not in terra_to_dbGap:
                print(project_id, 'not in spreadsheet?')
                project['dbGAP_study_id'] = None
                continue
            project['dbGAP_study_id'] = terra_to_dbGap[project_id]['study_id']

    with open('notebooks/figures/report-data.json', 'w') as fp:
        json.dump(report_data, fp)

    keys = ['project_id', 'dbGAP_project_id', 'accession', 'terra_sample_count', 'dbGAP_sample_count']
    with open('notebooks/figures/report-dbGAP.tsv', 'w') as fp:
        fp.write('\t'.join(keys))
        fp.write('\n')
        for project in report_data['projects']:            
            fp.write(f"{project['project_id']}\t")
            fp.write(f"{project['dbGAP_study_id']}\t")
            fp.write(f"{project['dbGAP_acession']}\t")
            fp.write(f"{list(filter(lambda p : (p['type']=='Sample'), project['nodes']))[0]['count']}\t")
            fp.write(f"{project['dbGAP_sample_count']}")
            fp.write('\n')



if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    main()
