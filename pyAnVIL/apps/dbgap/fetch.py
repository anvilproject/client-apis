from ftplib import FTP
import xmltodict

def fetch_schemas(projects=[]):
    """Retrieves schema from most recent version of project."""
    # TODO check out this API
    #  https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs000160
    #  returns a 302 to latest version: study.cgi?study_id=phs000160.v3.p1
    #  then
    # https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id=phs000001.v3.p1&rettype=xml 
    # login to schema
    ftp = FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login()
    schemas = {}
    project_ids_with_errors = []
    for project in projects:
        try:
            print(f"fetching {project}")
            schemas[project] = {'entities': {}, 'errors': [], 'counts': {}}
            ftp.cwd(f'/dbgap/studies/{project}/')
            # get most recent version
            files = ftp.nlst()
            version = files[-1]
            schemas[project]['version'] = version
            ftp.cwd(f'/dbgap/studies/{project}/{version}')
            # get gap exchange
            files = ftp.nlst()
            gap_exchange_path = [f for f in files if 'GapExchange' in f][0]
            schemas[project]['gap_exchange_path'] = gap_exchange_path
            with open(f"data/{gap_exchange_path}", 'wb') as fp:
                ftp.retrbinary(f'RETR {gap_exchange_path}', fp.write)
            # get StudyNameEntrez
            with open(f"data/{gap_exchange_path}") as fd:
                gap_exchange = xmltodict.parse(fd.read())
                schemas[project]['StudyNameEntrez'] = [s['Configuration']['StudyNameEntrez'] for k,s in gap_exchange['GaPExchange']['Studies'].items()][0]
            # get varaible summaries to create data dict
            ftp.cwd(f'/dbgap/studies/{project}/{version}/pheno_variable_summaries')
            files = ftp.nlst()
            data_dict_paths = [f for f in files if 'data_dict' in f]
            if len(data_dict_paths) == 0:
                schemas[project]['errors'].append(f"No data_dict found in /dbgap/studies/{project}/{version}/pheno_variable_summaries")
            for data_dict_path in data_dict_paths:
                entity = data_dict_path.split('.')[4]
                schemas[project]['entities'][entity] = {'data_dict_path': data_dict_path}
                with open(f"data/{data_dict_path}", 'wb') as fp:
                    ftp.retrbinary(f'RETR {data_dict_path}', fp.write)
                with open(f"data/{data_dict_path}") as fd:
                    data_dict = xmltodict.parse(fd.read())
                    schemas[project]['entities'][entity]['data_dict'] = [(v['name'], v['description'], v.get('type', None)) for v in data_dict['data_table']['variable']]
            # get variable reports
            var_report_paths = [f for f in files if 'var_report' in f]
            if len(var_report_paths) == 0:
                schemas[project]['errors'].append(f"No var_report found in /dbgap/studies/{project}/{version}/pheno_variable_summaries")
            for var_report_path in var_report_paths:
                entity = var_report_path.split('.')[5]
                schemas[project]['counts'][entity] = {'var_report_path': var_report_path}
                with open(f"data/{var_report_path}", 'wb') as fp:
                    ftp.retrbinary(f'RETR {var_report_path}', fp.write)
                with open(f"data/{var_report_path}") as fd:
                    var_report = xmltodict.parse(fd.read())
                    schemas[project]['counts'][entity]['var_report'] = {v['@var_name']: v['total'] for v in var_report['data_table']['variable'] if len(v['@id'].split('.')) == 3}
        except Exception as e:
            schemas[project]['errors'].append(str(e))
            project_ids_with_errors.append(project)
    return schemas, project_ids_with_errors
