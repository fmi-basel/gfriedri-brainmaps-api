import json
import requests

from google.oauth2 import service_account
from google.auth.transport import requests as auth_request


class EmptyResponse(Exception):
    pass


class AuthenticatedCall:
    """Handles BrainMaps API access and templates get and post requests

    To counter response failures the requests are repeated up to max_repeat
    times or until success

    Attributes:
        max_repeat (int) : number of repetitions for API calls
        _scoped_credentials : authorization credentials for identification
                                through Google OAuth2
        _headers (dict) : request headers
    """

    def __init__(self, service_account_secrets, max_repeat=10):
        self.max_repeat = max_repeat
        credentials = service_account.Credentials.from_service_account_file(
            service_account_secrets)
        scopes = ['https://www.googleapis.com/auth/brainmaps']
        self._scoped_credentials = credentials.with_scopes(scopes)
        self._headers = {}

        self.update_token()

    def update_token(self):
        """creates an access token and sets up request headers"""
        self._scoped_credentials.refresh(auth_request.Request())

        self._headers = {
            'Authorization': 'Bearer ' + self._scoped_credentials.token,
            'Content-type': 'application/json',
            'Accept-Encoding': 'gzip',
        }

    def get_request(self, url, query_param=''):
        """Get request function"""
        for i in range(self.max_repeat):
            if self._scoped_credentials.expired:
                self.update_token()
            resp = requests.get(url, params=query_param, headers=self._headers)
            if resp.ok:
                break
        if not resp.ok:
            print(resp.content)
            resp.raise_for_status()

        return resp

    def post_request(self, url, req_body):
        """Post request function"""
        for i in range(self.max_repeat):
            if self._scoped_credentials.expired:
                self.update_token()
            resp = requests.post(url, data=json.dumps(req_body).encode('utf-8'),
                                 headers=self._headers)
            if resp.ok:
                break
        if not resp.ok:
            print(resp.content)
            resp.raise_for_status()

        return resp


class BrainMapsRequest:
    """Base class for BrainMaps API functions, retrieves project information.

    for API reference see: https://developers.google.com/brainmaps (requires
    being logged in with Google account with access to the BrainMaps API)

    Attributes:
        base_url (str) : base url of API functions
        projects (list) : list of volumes available for a given user account,
                        list will be retrieved form server upon first request
                        and cached for later requests
        volumes (list) : list of volumes available for a given user account,
                        list will be retrieved form server upon first request
                        and cached for later requests
    """

    def __init__(self,
                 service_account_secrets,
                 volume_id=None,
                 change_stack_id=None,
                 project_id=None,
                 **kwargs):

        self.base_url = 'https://brainmaps.googleapis.com/v1beta2'
        self.volume_id = volume_id
        self.change_stack_id = change_stack_id
        self.project_id = project_id
        self._volumes = None
        self._projects = None

        # instantiate caller
        self._caller = AuthenticatedCall(
            service_account_secrets=service_account_secrets)
        if 'max_repeat' in kwargs.keys():
            self._caller.max_repeat = kwargs['max_repeat']

        # get/post aliases
        self.get_request = self._caller.get_request
        self.post_request = self._caller.post_request

    @property
    def projects(self):
        """retrieves list of projects available for a given user account

        Returns:
            self._projects (list) : list of available volumes"""
        if not self._projects:
            print('retrieving project list ...')
            url = self.base_url + '/projects'
            resp = self.get_request(url)
            self._projects = resp.json()['project']
        return self._projects

    @property
    def volumes(self):
        """retrieves list of volumes available for a given user account

        Returns:
            self._volumes (list) : list of available volumes"""
        if not self._volumes:
            print('retrieving volume list ...')
            url = self.base_url + '/volumes'
            resp = self.get_request(url)
            self._volumes = resp.json()['volumeId']
        return self._volumes

    def get_datasets(self, project_id=None):
        """lists data_sets in a given project

        Args:
            project_id (str) : self.project_id or any project_id specified

        Returns:
            list : list of datasets in a given project"""
        if not project_id:
            project_id = self.project_id
        if project_id:
            query_param = {'projectId': project_id}
            url = self.base_url + '/datasets'
            print('retrieving dataset list ...')
            resp = self.get_request(url, query_param=query_param)
            return resp.json()['datasetIds']
        else:
            print('project_id must be specified')
            return

    def volume_info(self, volume_id=None):
        """retrieves information about the segmentation volume

        Args:
            volume_id (str, optional) : volume_id specified in form of
                                        "projectId:datasetId:volumeId",
                                        default =self.volume_id

        Returns:
            list : example:
                        '[{'volumeSize': {'x': '10240', 'y': '17664',
                                    'z': '3840'},
                        'channelCount': '1',
                        'channelType': 'UINT64',
                        'pixelSize': {'x': 18, 'y': 18, 'z': 25},
                        'boundingBox': [{'corner': {'z': '32'},
                        'size': {'x': '10240', 'y': '17664', 'z': '3776'}}]}]
        """
        if not volume_id:
            volume_id = self.volume_id
        if volume_id:
            url = self.base_url + '/volumes/{}'.format(volume_id)
            print('retrieving volume info ...')
            resp = self.get_request(url)
            if not resp.json():
                raise EmptyResponse('The API response is empty')
            return resp.json()['geometry']
        else:
            print('volume_id must be specified')
            return

    def chg_stack_list(self, volume_id=None):
        """lists change stacks of a given volume

        Args:
            volume_id (str, optional) : volume_id specified in form of
                                        "projectId:datasetId:volumeId",
                                        default =self.volume_id

        Returns:
            list : list of change stack ids of a given volume"""
        if not volume_id:
            volume_id = self.volume_id
        if volume_id:
            url = self.base_url + '/changes/{}/change_stacks'.format(volume_id)
            print('retrieving change stack list ...')
            resp = self.get_request(url)
            return resp.json()['changeStackId']
        else:
            print('volume_id must be specified')
            return

    def chg_stack_metadata(self, volume_id=None, change_stack_id=None):
        """metadata for a given change stack

        Args:
            volume_id (str, optional) : volume_id specified in form of
                                        "projectId:datasetId:volumeId",
                                        default =self.volume_id

            change_stack_id (str, optional) : change_stack_id
                                            default =self.change_stack_id

        Returns:
            dict : dict with the change stack metadata"""
        if not volume_id:
            volume_id = self.volume_id
        if not change_stack_id:
            change_stack_id = self.change_stack_id
        if volume_id and change_stack_id:
            url = self.base_url + '/changes/{}/{}:stack_metadata'.format(
                volume_id, change_stack_id)
            print('retrieving change stack metadata ...')
            resp = self.get_request(url)
            return resp.json()['metadata']
        else:
            print('volume_id and change_stack_id have to be specified')
            return

    def create_chg_stack(self, change_stack_id, volume_id=None):
        """creates a change stack under a given volume

        Args:
            change_stack_id (str): identifier of the changestack
            volume_id (str, optional) : volume_id specified in form of
                                        "projectId:datasetId:volumeId",
                                        default =self.volume_id
        Returns:
            dict : dict with the response of post request
        """
        if not volume_id:
            volume_id = self.volume_id
        url = self.base_url + '/changes/{}/{}:create'.format(
            volume_id, change_stack_id)
        body = {}
        print('creating change stack {} for volume {}'.format(change_stack_id,
                                                              volume_id))
        resp = self.post_request(url, body)
        return resp.json()
