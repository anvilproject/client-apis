from anvil.gen3_auth import Gen3TerraAuth
from gen3.submission import Gen3Submission


def test_gen3_terra_auth():
    """Validates retrieving access_token and passing to gen3."""
    auth = Gen3TerraAuth()
    endpoints = ["https://gen3.datastage.io", "https://staging.theanvil.io", "https://api.gdc.cancer.gov"]

    for endpoint in endpoints:
        try:
            submission_client = Gen3Submission(endpoint, auth)
            programs = submission_client.get_programs()
            assert len(programs) > 0, f'Should have programs {endpoint}'
            # parse program name from links
            programs = [p.split('/')[-1] for p in programs['links']]
            for program in programs:
                projects = submission_client.get_projects(program)
                assert len(projects) > 0, f'Should have projects {program}'
            print(f'OK: Authenticated from {auth._terra_auth_url} to {endpoint} len(projects): {len(projects)}')
        except Exception as e:
            print(f'ERROR: Could not authenticate from {auth._terra_auth_url} to {endpoint} {str(e)}')
