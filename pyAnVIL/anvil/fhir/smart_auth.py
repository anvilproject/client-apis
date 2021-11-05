# -*- coding: utf-8 -*-
"""Google gcloud access_token handling class for smart-on-fhir/client-py FHIR client."""
import logging
from subprocess import Popen, PIPE

from fhirclient import auth

logger = logging.getLogger(__name__)


class FHIRAuthError(Exception):
    """Reports any problem retrieving access token from gcloud."""

    pass


class GoogleFHIRAuth(auth.FHIRAuth):
    """Google gcloud access_token handling class for smart-on-fhir/client-py FHIR client.

    Requires:
        `pip install -e git+https://github.com/smart-on-fhir/client-py#egg=fhirclient`

    :param access_token: Optional access token, if none provided `gcloud auth print-access-token` is used

    Examples:
        from fhirclient import client
        from anvil.fhir.smart_auth import GoogleFHIRAuth
        settings = {
            'app_id': 'my_web_app',
            'api_base': 'https://healthcare.googleapis.com/v1/projects/gcp-testing-308520/locations/us-east4/datasets/testset/fhirStores/fhirstore/fhir'
        }
        smart = client.FHIRClient(settings=settings)
        # optionally pass token
        smart.server.auth = GoogleFHIRAuth(access_token=''ya29.abcd...')
        smart.prepare()
        assert smart.ready, "server should be ready"
        # search for all ResearchStudy
        import fhirclient.models.researchstudy as rs
        [s.title for s in rs.ResearchStudy.where(struct={}).perform_resources(smart.server)]
        >>>
        ['1000g-high-coverage-2019', 'my NCPI research study example']
    """

    auth_type = 'bearer'

    def __init__(self, state=None, access_token=None):
        """Initialize access_token, call super."""
        self.access_token = access_token
        if not self.access_token:
            self.access_token = self._get_auth_value()
        super(GoogleFHIRAuth, self).__init__(state=state)

    @property
    def ready(self):
        """Return True if access_token exists."""
        return True if self.access_token else False

    def reset(self):
        """Clear access_token."""
        super(GoogleFHIRAuth, self).reset()
        self.access_token = None

    def can_sign_headers(self):
        """Return True if access_token exists."""
        return True if self.access_token is not None else False

    def signed_headers(self, headers):
        """Return updated HTTP request headers, if possible, raises if there is no access_token."""
        if not self.can_sign_headers():
            raise Exception("Cannot sign headers since I have no access token")

        if headers is None:
            headers = {}
        headers['Authorization'] = "Bearer {0}".format(self.access_token)

        return headers

    def reauthorize(self, server):
        """Perform reauthorization.

        Args:
            - server - The Server instance to use

        Returns:
            - output - The launch context dictionary, or None on failure
        """
        logger.debug("SMART AUTH: Refreshing token")
        self.access_token = self._get_auth_value()
        return {'access_token': self._access_token}

    def handle_401(self, response, **kwargs):
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

        self.access_token = None
        logger.debug("handle_401, cleared _access_token, retrying with new token")

        self.access_token = self._get_auth_value()
        newreq.headers["Authorization"] = "Bearer {0}".format(self.access_token)

        _response = response.connection.send(newreq, **kwargs)
        _response.history.append(response)
        _response.request = newreq

        return _response

    @property
    def state(self):
        """Save state."""
        s = super(GoogleFHIRAuth, self).state
        if self.access_token is not None:
            s['access_token'] = self.access_token

        return s

    def from_state(self, state):
        """Update ivars from given state information."""
        super(GoogleFHIRAuth, self).from_state(state)
        self.access_token = state.get('access_token') or self.access_token

    def _get_auth_value(self):
        """Return the Authorization header value for the request.

        This gets called when added the Authorization header to the request.
        This fetches the access token from the refresh token if the access token is missing.

        """
        if not self.access_token:
            try:
                # get the local access token using gcloud
                cmd = ['gcloud', 'auth', 'print-access-token']
                logger.debug(f"getting gcloud_access_token {cmd}")
                p = Popen(cmd, stdout=PIPE, stderr=PIPE)
                gcloud_access_token, stderr = p.communicate()
                # remove CR
                gcloud_access_token = gcloud_access_token.decode("utf-8").rstrip()
                assert len(gcloud_access_token) > 0, f'get gcloud_access_token MUST have an access token {stderr}'
                logger.debug(f"gcloud_access_token {gcloud_access_token}")
                self.access_token = gcloud_access_token
                if logger.level == logging.DEBUG:
                    logger.debug(self.access_token)
                    print(self.access_token)
                    # expires_at = datetime.fromisoformat(self.access_token['expires_at'])
                    # logger.debug(f'access token expires in {str(expires_at - now)}')
                    # # add padding
                    # logger.debug(base64.b64decode(self._access_token.split('.')[1] + "==="))
            except Exception as e:
                raise FHIRAuthError(f"Failed to get gcloud_access_token {e}")
        return self.access_token


# register class
GoogleFHIRAuth.register()
