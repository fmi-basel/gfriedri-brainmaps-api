import networkx as nx
import numpy as np
import struct
from brainmaps_api_fcn.basic_requests import BrainMapsRequest, EmptyResponse


class Meshes(BrainMapsRequest):
    def __init__(self, service_account_secrets, volume_id, **kwargs):
        super(Meshes, self).__init__(
            service_account_secrets=service_account_secrets,
            volume_id=volume_id, **kwargs)

    @property
    def mesh_base_url(self):
        return self.base_url + '/objects/{}/meshes'.format(self.volume_id)

    def _get_mesh_name(self, mesh_type='TRIANGLES'):
        """retrieves name of the mesh collection for the segmentation volume

        Args:
            mesh_type (str) : indicates mesh type for which collection name is
                                retrieved: 'TRIANGLES' for volumetric meshes and
                                'LINE_SEGMENTS' for skeleton meshes
        Returns:
            str : mesh_name
        """
        url = self.mesh_base_url
        resp = self.get_request(url)
        if not resp.json():
            raise EmptyResponse('The API response is empty')
        for x in resp.json()['meshes']:
            if x['type'] == mesh_type:
                return x['name']
        else:
            msg = 'Meshes of type ' + mesh_type + ' not found for volume ' + \
                  self.volume_id
            raise ValueError(msg)

    def _get_fragment_list(self, sv_id, mesh_name):
        """gets list of mesh fragments associated with segment sv_id

        Args:
            sv_id (int) : segment id
            mesh_name (str) : name of the mesh collection associated with the
                                segmentation volume
        Returns:
            fragments (list) : list of mesh fragment ids
        """
        url = self.mesh_base_url + '/{meshName}:listfragments'.format(
            meshName=mesh_name)
        query_param = {'returnSupervoxelIds': True,
                       'objectId': str(sv_id)}
        resp = self.get_request(url, query_param)
        # unknown sv_id returns fragmentKey = ['0000000000000000'] not an error
        if not resp.json() or resp.json()['fragmentKey'] == [
            '0000000000000000'
        ]:
            raise EmptyResponse(
                'The API request did not return anything (useful)')

        return resp.json()['fragmentKey']

    # TRIANGLE MESHES
    @staticmethod
    def _mesh_from_stream(bytestream):
        """reads out a single mesh fragment from bytestream

        Args:
            bytestream (bytearray) : contains mesh information

        Returns:
            bytestream (bytearray) : contains information of remaining fragments
                                    of the mesh
            indices (list) : list of mesh triangle indices
            vertices (list) : node position in voxel coordinates
        """
        print(' in _mesh_from_stream: bytestream length', len(bytestream))
        ull_size = 8
        float_size = 4
        id_length = struct.unpack("<Q", bytestream[ull_size:ull_size * 2])[0]
        del bytestream[:ull_size * 2 + id_length]
        no_vert = struct.unpack("<Q", bytestream[:ull_size])[0]
        del bytestream[:ull_size]
        no_idx = struct.unpack("<Q", bytestream[:ull_size])[0]
        del bytestream[:ull_size]
        vertices = list(
            struct.unpack('<' + str(no_vert * 3) + 'f',
                          bytestream[:float_size * no_vert * 3]))
        del bytestream[:float_size * no_vert * 3]
        indices = list(
            struct.unpack("<" + str(no_idx * 3) + 'i',
                          bytestream[:float_size * no_idx * 3]))
        del bytestream[:float_size * no_idx * 3]
        return bytestream, indices, vertices

    def _get_mesh_fragment(self, sv_id, mesh_name, fragments):
        """gets list of mesh fragments associated with segment sv_id

        Args:
            sv_id (int) : segment id
            mesh_name (str) : name of the mesh collection associated with the
                                segmentation volume
            fragments (list) : list of mesh fragment ids

        Returns:
            bytearray containing the information of the triangle mesh
            represenation
        """
        url = self.base_url + '/objects/meshes:batch'
        batchmeshfragment = {'objectId': str(sv_id),
                             'fragmentKeys': fragments}
        req_body = {
            'volumeId': self.volume_id,
            'meshName': mesh_name,
            'batches': [batchmeshfragment]
        }
        resp = self.post_request(url, req_body)
        if not any(resp.content):
            raise EmptyResponse(
                'The API response is empty. Check input variables')
        return bytearray(resp.content)

    def download_mesh(self, sv_id):
        """Returns the mesh for segment sv_id

        Meshes of one segment are usually split into several fragments. To
        retrieve the mesh of a particular segment one needs to first retrieve
        the name of the mesh collection associated with a segmentation volume.
        From this the list of fragments into which segment sv_id has been split
        can be retrieved in order to download the meshes.

        Args:
            sv_id (int) : segment id
            volume_id (str) : id of the base segmentation volume
            change_stack_id (str) : id of the agglomeration change stack
            service_account (str) : path to the service account json in order to
                                    authenticate through Google OAuth2

        Returns:
            vertices (np.array) : all vertices of the mesh in [x,y,z] voxel
                                    coordinates
            indices (np.array) : indices of the mesh
        """
        mesh_name = self._get_mesh_name()
        fragments = self._get_fragment_list(sv_id, mesh_name)
        bytestream = self._get_mesh_fragment(sv_id, mesh_name, fragments)
        vertices = []
        indices = []
        for _ in range(len(fragments)):
            bytestream, ind, vert = self._mesh_from_stream(bytestream)
            vertices += (vert)
            indices += ind

        vertices = np.array(vertices).reshape(max(1, int(len(vertices) / 3)), 3)
        indices = np.array(indices).reshape(max(1, int(len(indices) / 3)), 3)
        return vertices, indices

    # SKELETONS
    @staticmethod
    def _graph_from_skel_json(skel_json):
        """Creates a networkx graph from skeleton json

        Args:
            skel_json : json with skeleton nodes in world coordinates
            ('vertices') and edges ('indices')

        Returns:
            skel_graph (networkx graph) : skeleton graph, nodes x,y,z
                                        coordinates in attribute "pos",  edges
                                        store edge length in weight attribute
        """
        skel_graph = nx.DiGraph()
        nnodes = int(len(skel_json['skeleton']['vertices']) / 3)
        for i in range(nnodes):
            idx = 3 * i
            skel_graph.add_node(i,
                                pos=skel_json['skeleton']['vertices'][idx:3 +
                                                                          idx])
        nedges = int(len(skel_json['skeleton']['indices']) / 2)
        for i in range(nedges):
            idx = 2 * i
            source_node = skel_json['skeleton']['indices'][idx]
            target_node = skel_json['skeleton']['indices'][idx + 1]
            dist = np.linalg.norm([
                x1 - x2 for x1, x2 in zip(skel_graph.nodes[target_node]['pos'],
                                          skel_graph.nodes[source_node]['pos'])
            ])
            skel_graph.add_edge(source_node, target_node, weight=dist)
        return skel_graph

    def _fetch_skeleton(self, sv_id, mesh_name):
        """Downloads skeleton for single segment

        Args:
            sv_id (int) : segment id
            mesh_name (str) : name of the mesh collection associated with the
                                segmentation volume

        Returns:
            Structure of the skeleton json returned from API:
            "vertices" : list of node position in form of [x,y,z] coordinates
                        (world coordinates not voxel coordinates)
            "indices" : list of edges [node pairs]
        """
        query_param = {'objectId': str(sv_id)}
        url = self.mesh_base_url + '/{mesh_name}/skeleton:get'.format(
            mesh_name=mesh_name)
        resp = self.get_request(url, query_param)

        if not resp.json():
            raise EmptyResponse(
                'The API response is empty. Check input variables')
        return resp.json()

    def download_skeleton(self, sv_id):
        """Downloads skeleton of segment sv_id and returns it as graph

        Args:
            sv_id (int) : segment id
            volume_id (str) : id of the base segmentation volume
            change_stack_id (str) : id of the agglomeration change stack
            service_account (str) : path to the service account json in order to
                                    authenticate through Google OAuth2

        Returns:
            networkx graph : skeleton graph, nodes x,y,z coordinates in
                            attribute "pos",  edges store edge length in weight
                            attribute
         """
        mesh_name = self._get_mesh_name(mesh_type='LINE_SEGMENTS')
        skel_json = self._fetch_skeleton(sv_id, mesh_name)
        return self._graph_from_skel_json(skel_json)
