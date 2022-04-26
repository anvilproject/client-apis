# -*- coding: utf-8 -*-
"""Google gcloud access_token handling class for smart-on-fhir/client-py FHIR client."""
import logging
from subprocess import Popen, PIPE

from fhirclient import auth

logger = logging.getLogger(__name__)

REGISTERED = []


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
        from anvil.clients.smart_auth import GoogleFHIRAuth
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
            logger.debug('Getting access token')
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
if GoogleFHIRAuth not in REGISTERED:
    # print('GoogleFHIRAuth registered')
    GoogleFHIRAuth.register()
    REGISTERED.append(GoogleFHIRAuth)


class KidsFirstFHIRAuth(auth.FHIRAuth):
    """KidsFirst handling class for smart-on-fhir/client-py FHIR client.

    Requires:
        `pip install -e git+https://github.com/smart-on-fhir/client-py#egg=fhirclient`

    :param cookie: Mandatory AWSELBAuthSessionCookie token.

    Examples:
        cookie = 'AWSELBAuthSessionCookie-0=ABC123abc...'

        from fhirclient import client
        from anvil.clients.smart_auth import KidsFirstFHIRAuth
        settings = {
            'app_id': 'my_web_app',
            'api_base': 'https://kf-api-fhir-service.kidsfirstdrc.org'
        }
        kids_first = client.FHIRClient(settings=settings)
        # optionally pass token
        kids_first.server.auth = KidsFirstFHIRAuth(cookie=cookie)
        kids_first.prepare()
        assert kids_first.ready, "server should be ready"
        # search for all ResearchStudy
        import fhirclient.models.researchstudy as rs
        [s.title for s in rs.ResearchStudy.where(struct={}).perform_resources(kids_first.server)]
        >>>
        ['OpenDIPG: ICR London', 'Genomic Studies of Orofacial Cleft Birth Defects', 'Genome-wide Sequencing to Identify the Genes Responsible for Enchondromatoses and Related Malignant Tumors', 'Kids First: Craniofacial Microsomia: Genetic Causes and Pathway Discovery', 'Whole Genome Sequencing of African and Asian Orofacial Clefts Case-Parent Triads', 'Kids First: Genetics of Structural Defects of the Kidney and Urinary Tract', 'National Heart, Lung, and Blood Institute (NHLBI) Bench to Bassinet Program: The Gabriella Miller Kids First Pediatric Research Program of the Pediatric Cardiac Genetics Consortium (PCGC)', 'Kids First: Genetics at the Intersection of Childhood Cancer and Birth Defects', 'TARGET: Neuroblastoma (NBL)', 'Genomic Analysis of Familial Leukemia', 'Kids First: Genomics of Orofacial Cleft Birth Defects in Latin American Families', 'Genomics of Orthopaedic Disease Program', 'Kids First: The Genetics of Microtia in Hispanic Populations', 'Gabriella Miller Kids First Pediatric Research Program in Novel Cancer Susceptibility in Families (from BASIC3)', 'Genomic Analysis of Esophageal Atresia and Tracheoesophageal Fistulas and Associated Congenital Anomalies', 'Discovering the Genetic Basis of Human Neuroblastoma: A Gabriella Miller Kids First Pediatric Research Program (Kids First) Project', 'Kids First: Genomic Analysis of a Cohort with Infantile Hemangiomas Associated with Multi-organ Structural Birth Defects', 'Pharmacokinetics, Pharmacodynamics, and Safety Profile of Understudied Drugs Administered to Children per Standard of Care (POP02)', "Pediatric Brain Tumor Atlas - Children's Brain Tumor Tissue Consortium", 'BCH Structural Birth Defects Collaboration: Syndromic cranial dysinnervation disorders', 'Expanded Ewing sarcoma cohort for tumor genomics and association with DNA repair deficiencies, clinical presentation, and outcome', 'Genetic Basis of Disorders/Differences of Sex Development (DSD)', 'Pediatric Brain Tumor Atlas: PNOC', 'Kids First: Whole genome sequencing of nonsyndromic craniosynostosis', 'Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome', 'TARGET: Acute Myeloid Leukemia (AML)', 'Kids First: Germline and Somatic Variants in Myeloid Malignancies in Children', 'Genomic Analysis of Congenital Diaphragmatic Hernia', 'An Integrated Clinical and Genomic Analysis of Treatment Failure in Pediatric Osteosarcoma']
    """

    auth_type = 'cookie'

    def __init__(self, state=None, cookie=None):
        """Initialize access_token, call super."""
        self.cookie = cookie
        super(KidsFirstFHIRAuth, self).__init__(state=state)

    @property
    def ready(self):
        """Return True if access_token exists."""
        return True if self.cookie else False

    def reset(self):
        """Clear access_token."""
        super(KidsFirstFHIRAuth, self).reset()
        self.cookie = None

    def can_sign_headers(self):
        """Return True if access_token exists."""
        return True if self.cookie is not None else False

    def signed_headers(self, headers):
        """Return updated HTTP request headers, if possible, raises if there is no access_token."""
        if not self.can_sign_headers():
            raise Exception("Cannot sign headers since I have no cookie")

        if headers is None:
            headers = {}
        headers['cookie'] = self.cookie
        logger.debug(headers)
        return headers

    def handle_callback(self, url, server):
        """Return the launch context."""
        logger.debug('handle_callback')
        raise Exception(f"{self} cannot handle callback URL")

    def reauthorize(self, server):
        """Perform reauthorization.

        Args:
            - server - The Server instance to use

        Returns:
            - output - The launch context dictionary, or None on failure
        """
        logger.debug("SMART AUTH: Refreshing token")
        return {'cookie': self.cookie}

    def handle_401(self, response, **kwargs):
        """Handle failed requests when authorization failed.

        This gets called after a failed request when an HTTP 401 error
        occurs. This then tries to refresh the access token in the event
        that it expired.

        Args:
            request (object): The failed request object

        """
        for keyword in ['authorize', 'login']:
            if keyword in response.url:
                raise Exception("The token is no longer valid, please re-harvest from KidsFirst browser.")
        if not response.status_code == 401 and not response.status_code == 403:
            return response
        raise Exception("The user represented by this cookie does not have access.")

    @property
    def state(self):
        """Save state."""
        s = super(KidsFirstFHIRAuth, self).state
        if self.cookie is not None:
            s['cookie'] = self.cookie

        return s

    def from_state(self, state):
        """Update ivars from given state information."""
        super(KidsFirstFHIRAuth, self).from_state(state)
        self.cookie = state.get('cookie') or self.cookie


# register class
if KidsFirstFHIRAuth not in REGISTERED:
    # print('KidsFirstFHIRAuth registered')
    KidsFirstFHIRAuth.register()
    REGISTERED.append(KidsFirstFHIRAuth)
