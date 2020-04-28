import requests
import requests_cache
import json
import xmltodict
requests_cache.install_cache('dbgap')


url_base = 'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id='

def get_sample_count(project_id):
    """Returns qualified study, number of samples."""
    try:
        r = requests.get(f"{url_base}{project_id}", allow_redirects=False)
        assert r.status_code == 302, r.status_code
        qualified_study = r.headers['location'].split('=')[1]
        r = requests.get(f"https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id={qualified_study}&rettype=xml")
        assert r.status_code == 200, r.status_code
        d = xmltodict.parse(r.text)
        return ([qualified_study, len(d['DbGap']['Study']['SampleList']['Sample'])])
    except Exception as e:
        print(f"{project_id}/{qualified_study}/{e}")
        # raise
        return None

