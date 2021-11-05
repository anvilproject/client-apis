"""Gen3 auth helper class for use within the AnVIL (terra, gen3) environment."""

import logging
import requests
import base64

from datetime import datetime
from subprocess import Popen, PIPE
from requests.auth import AuthBase


TERRA_TOKEN_URL = "https://broad-bond-prod.appspot.com/api/link/v1/anvil/accesstoken"


class AnVILAuthError(Exception):
    """Reports any problem retrieving access token from terra."""

    pass


class Gen3TerraAuth(AuthBase):
    """Gen3 auth helper class for use with requests auth.

    Implements requests.auth.AuthBase in order to support JWT authentication.
        * Depends on gcloud cli and valid configuration
        * Validates with terra using token provided by 'gcloud auth print-access-token'
        * Queries terra endpoint for fence access_token.
        * Intercepts all calls to gen3 and inserts Bearer token
        * Automatically refreshes access tokens when they expire.

    Args:
        * terra_auth_url (str): URL of the terra endpoint Default: "https://broad-bond-prod.appspot.com/api/link/v1/anvil/accesstoken".
        * user_email (str): Optional, google id to pass to 'gcloud auth print-access-token' Default: None.

    Examples: ::

        # authenticate with terra
        from anvil.clients.gen3_auth import Gen3TerraAuth
        from gen3.submission import Gen3Submission

        gen3_endpoint = "https://gen3.theanvil.io"
        auth = Gen3TerraAuth(endpoint=gen3_endpoint)
        submission_client = Gen3Submission(gen3_endpoint, auth)
        query = '{project(first:0) {code,  subjects {submitter_id}, programs {name}  }}'
        results = submission_client.query(query)
        [p['code'] for p in results['data']['project']]
        >>> ['GTEx', '1000Genomes']

        # access terra api
        from anvil.terra.api import whoami
        whoami()
        >>> 'anvil.user@gmail.com'

    """

    def __init__(self, endpoint, terra_auth_url=TERRA_TOKEN_URL, user_email=None):
        """Initialize properties."""
        self._access_token = None
        self._terra_auth_url = terra_auth_url
        assert self._terra_auth_url, "MUST have _terra_auth_url"
        self._user_email = user_email
        self.endpoint = endpoint
        self._logger = logging.getLogger(__name__)

    def __call__(self, request):
        """Add authorization header to the request.

        This gets called by the python.requests package on outbound requests
        so that authentication can be added.

        Args:
            request (object): The incoming request object

        """
        self._logger.debug(f'__call__, {request.url} adding Authorization header')
        request.headers["Authorization"] = self._get_auth_value()
        request.register_hook("response", self._handle_401)
        return request

    def _handle_401(self, response, **kwargs):
        """Handle failed requests when authorization failed.

        This gets called after a failed request when an HTTP 401 error
        occurs. This then tries to refresh the access token in the event
        that it expired.

        Args:
            request (object): The failed request object

        """
        if not response.status_code == 401 and not response.status_code == 403:
            return response

        # Free the original connection
        response.content
        response.close()

        # copy the request to resend
        newreq = response.request.copy()

        self._access_token = None
        self._logger.debug("_handle_401, cleared _access_token, retrying with new token")

        newreq.headers["Authorization"] = self._get_auth_value()

        _response = response.connection.send(newreq, **kwargs)
        _response.history.append(response)
        _response.request = newreq

        return _response

    def _get_auth_value(self):
        """Return the Authorization header value for the request.

        This gets called when added the Authorization header to the request.
        This fetches the access token from the refresh token if the access token is missing.

        """
        if not self._access_token:
            try:
                # get the local access token using gcloud
                cmd = ['gcloud', 'auth', 'print-access-token']
                if self._user_email:
                    cmd.append(self._user_email)

                self._logger.debug(f"get gcloud_access_token {cmd}")
                p = Popen(cmd, stdout=PIPE, stderr=PIPE)
                gcloud_access_token, stderr = p.communicate()
                gcloud_access_token = gcloud_access_token.decode("utf-8").rstrip()
                assert len(gcloud_access_token) > 0, f'get gcloud_access_token MUST have an access token {stderr}'
                self._logger.debug(f"gcloud_access_token {gcloud_access_token}")
                # authenticate to terra, ask for fence/accesstoken
                headers = {'Authorization': f'Bearer {gcloud_access_token}'}
                r = requests.get(self._terra_auth_url, headers=headers)
                assert r.status_code == 200, f'MUST respond with 200 {self._terra_auth_url} {r.text}'
                self._logger.debug(r.text)
                terra_access_token = r.json()
                assert len(terra_access_token['token']) > 0, 'MUST have an access token'
                assert len(terra_access_token['expires_at']) > 0, 'MUST have an expires_at '

                expires_at = datetime.fromisoformat(terra_access_token['expires_at'])
                now = datetime.now()
                assert expires_at > now, 'expires_at MUST be in the future'

                self._access_token = terra_access_token['token']

                if self._logger.level == logging.DEBUG:
                    self._logger.debug(f'Terra access token expires in {str(expires_at - now)}')
                    self._logger.debug(self._access_token)
                    # add padding
                    self._logger.debug(base64.b64decode(self._access_token.split('.')[1] + "==="))

            except Exception as e:
                raise AnVILAuthError(
                    "Failed to authenticate to {}\n{}".format(self._terra_auth_url, str(e))
                )

        return "Bearer " + self._access_token
