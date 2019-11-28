from brainmaps_api_fcn.basic_requests import BrainMapsRequest, EmptyResponse


def int_to_list(sv_id):
    """helper function to turn int input to list for request body creation
    in many BrainMapsAPI calls"""
    if type(sv_id) == int:
        return [sv_id]
    else:
        return sv_id


class EquivalenceRequests(BrainMapsRequest):
    """Collection of functions to read from and write to an agglomeration graph

    see also BrainMapsRequest

    terminology:
        equivalence = edge in agglomeration graph
        supervoxel = segment : can be node in agglomeration graph as well as an
                                agglomerated segment ~ connected component in
                                the agglomeration graph
    Attributes:
        volume_id (str) : id of the base segmentation volume
        change_stack_id (str) : id of the agglomeration change stack
        equ_base_url (str) : base url of equivalence functions
    """

    def __init__(self, service_account_secrets, volume_id, change_stack_id,
                 **kwargs):
        super(EquivalenceRequests, self).__init__(service_account_secrets,
                                                  volume_id=volume_id,
                                                  change_stack_id=change_stack_id,
                                                  **kwargs)

    @property
    def equ_base_url(self):
        return self.base_url + '/changes/{}/{}/equivalences:'.format(
            self.volume_id, self.change_stack_id)

    def set_equivalence(self, edge):
        """sets an equivalence between two unagglomerated supervoxels

        Args:
            edge (list) : list with segment id pair or list with pair of
                        [x,y,z] location

        Returns:
            int : the (novel) common group_id after merging segments
        """
        # Check whether input is a list of locations or segment ids
        if all([type(item) == list and len(item) == 3 for item in edge]):
            body = {
                "edge": {
                    "firstLocation":
                        ', '.join(str(int(x)) for x in edge[0]),
                    "secondLocation":
                        ', '.join(str(int(x)) for x in edge[1]),
                }
            }
        else:
            body = {"edge": {"first": str(edge[0]), "second": str(edge[1])}}
        url = self.equ_base_url + 'set'
        resp = self.post_request(url, body)
        if not resp.json():
            raise EmptyResponse(
                'The API response is empty. Check input arguments')

        return int(resp.json()['groupId'])

    def get_equivalence_list(self, sv_id):
        """Downloads list of all edges of segments in sv_id

        Returns a list containing all edges of the supervoxels in sv_id. Edges
        between members of sv_id will only appear once ("undirected"). The edge
        list return is not sorted by sv_id entry.

        Args:
            sv_id (int or list) : segment ids

        Returns:
            edges (list) : list with all edges of segments in sv_id
        """
        body = {
            "segmentId": [str(x) for x in int_to_list(sv_id)],
            "returnMetadata": False,
        }
        url = self.equ_base_url + 'list'
        resp = self.post_request(url, body)
        if not resp.json():
            raise EmptyResponse('The API response is empty. Check input arguments')
        edges = []
        for edge_json in resp.json()['edge']:
            edges.append([int(edge_json['first']), int(edge_json['second'])])
        return edges

    def delete_equivalence(self, edge):
        """Deletes equivalence in the agglomeration graph

        Removes equivalence between two segments that were agglomerated.
        Edge input is composed of base volume ids, the order does not matter.
        If edge does not exist no error is thrown by API

        Args:
            edge (list) : pair of segment ids

        Returns:
            resp : post request response
        """
        body = {"edge": {"first": str(edge[0]), "second": str(edge[1])}}
        url = self.equ_base_url + 'delete'
        resp = self.post_request(url, body)
        return resp

    def multi_delete(self, edge_list):
        """Deletes equivalences in list in the agglomeration graph

        Removes equivalences between segments pairs that were agglomerated.
        Edge input is composed of base volume ids, the order does not matter.
        If edge does not exist no error is thrown by API

        Args:
            edge_list (list) : list of edges [[segment_id1, segment_id2],
                            [segment_id1, segment_id3], ...]

        Returns:
            resp : post request response
        """
        body = {"edge":
                    [{"first": str(edge[0]),
                      "second": str(edge[1])} for edge in edge_list]
                }
        url = self.equ_base_url + 'multidelete'
        resp = self.post_request(url, body)
        return resp

    def get_groups(self, sv_id):
        """Returns the list of all segments belonging to the same agglomerated
        supervoxel ids as the segment(s) in sv_id.

        Args:
            sv_id (int or list) : supervoxel ids

        Returns:
              members (list) : members of the agglomerated segments for the
              supervoxels in sv_id
        """
        body = {"segmentId": [str(x) for x in int_to_list(sv_id)]}
        url = self.equ_base_url + 'getgroups'
        resp = self.post_request(url, body)
        if not resp.json():
            raise EmptyResponse(
                'The API response is empty. Check input arguments')
        members = dict()
        for i, entry in enumerate(resp.json()['groups']):
            members[int_to_list(sv_id)[i]] = [
                int(x) for x in entry['groupMembers']
            ]
        return members

    def get_map(self, sv_id):
        """For each segment in sv_id the id of agglomerated supervoxel it
        belongs to is returned.

        Args:
            sv_id (int or list) : supervoxel id(s)

        Returns:
            group_ids (list) : list of agglomerated segment ids
        """
        body = {
            "segmentId": [str(x) for x in int_to_list(sv_id)],
        }
        url = self.equ_base_url + 'getmap'
        resp = self.post_request(url, body)
        if not resp.json():
            raise EmptyResponse(
                'The API response is empty. Check input arguments')
        mapping = resp.json()['mapping']
        group_ids = [min(int(x['first']), int(x['second'])) for x in mapping]
        return group_ids

    def isolate_set(self, sv_id, exclude=True):
        """ Splits all edges of the super voxels in sv_id.

        If several supervoxel ids are entered the exclude flag determines
        whether edges between supervoxels in sv_id are kept or split,
        default = True. Throws error if sv_id does not exist

        Args:
            sv_id (int or list) : supervoxel id(s)
            exclude (boolean) : flag that determines whether edges in between
                            members of sv_id are excluded from deletion
        Returns:
            resp : post request response
         """
        body = {
            "segmentIds": [str(x) for x in int_to_list(sv_id)],
            "excludeEdgesWithinSet": exclude
        }
        url = self.equ_base_url + 'isolateset'
        resp = self.post_request(url, body)
        return resp
