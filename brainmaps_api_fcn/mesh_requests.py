import networkx as nx
import numpy as np
import struct
from brainmaps_api_fcn.basic_requests import BrainMapsRequest, EmptyResponse


class Meshes(BrainMapsRequest):
    def __init__(self, service_account_secrets, volume_id, **kwargs):
        super(Meshes, self).__init__(
            service_account_secrets=service_account_secrets,
            volume_id=volume_id, **kwargs)
        if 'fragment_limit' in kwargs.keys():
            self.max_fragments = kwargs['fragment_limit']
        else:
            self.max_fragments = 256

    @property
    def mesh_base_url(self):
        return self.base_url + '/objects/{}/meshes'.format(self.volume_id)

    @staticmethod
    def _mesh_from_stream(bytestream):
        """reads out a single mesh fragment from bytestream

        Args:
            bytestream(bytearray): contains mesh information in following
                                   form:
                            1. 8 bytes: object ID in uint64
                            2. 8 bytes: length of fragment ID/name xx in uint64
                            3. xx entries: fragment ID/name in char -> 'xxs'
                            4. 8 bytes: number of vertices yy in uint64
                            5. 8 bytes: number of indices zz in uint64
                            6. 3*4 bytes*yy entries: vertices in XYZ triplets
                            7. 3*4 bytes*zz entries: indices in vertex triplets

        Returns:
            bytestream (bytearray) : contains information of remaining fragments
                                    of the mesh
            indices (list) : list of mesh triangle indices
            vertices (list) : node position in voxel coordinates
        """
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

        # convert to array and reshape
        vertices = np.array(vertices).reshape(max(1, no_vert),
                                              3).astype(int)
        indices = np.array(indices).reshape(max(1, no_idx),
                                            3).astype(int)

        return bytestream, indices, vertices

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
        name_list = []
        for x in resp.json()['meshes']:
            if x['type'] == mesh_type:
                name_list.append(x['name'])
        if name_list:
            return name_list
        else:
            msg = 'Meshes of type ' + mesh_type + ' not found for volume ' + \
                  self.volume_id
            raise ValueError(msg)

    # TRIANGLE MESHES
    def _get_fragment_list(self, segment_id, mesh_name, change_stack_id=None):
        """gets list of mesh fragments associated with segment sv_id

        Args:
            segment_id (int) : segment id
            mesh_name (str) : name of the mesh collection associated with the
                                segmentation volume
            change_stack_id (str, optional) : name of the change stack, if
                                              given entire fragment list of the
                                              agglomerated parent will be
                                              downloaded

        Returns:
            fragments (list) : list of mesh fragment ids
        """
        url = self.mesh_base_url + '/{meshName}:listfragments'.format(
            meshName=mesh_name)
        query_param = {'returnSupervoxelIds': True,
                       'objectId': str(segment_id)}
        if change_stack_id:
            query_param.update({'header.changeStackId': change_stack_id})
        resp = self.get_request(url, query_param)
        # unknown sv_id returns fragmentKey = ['0000000000000000'] not an error
        if not resp.json() or resp.json()['fragmentKey'] == [
            '0000000000000000'
        ]:
            raise EmptyResponse(
                'The API request did not return anything (useful)')

        return resp.json()['supervoxelId'], resp.json()['fragmentKey']

    def make_query_package(self, supervoxel_ids, fragments):
        """"""
        batches = []
        space_left = self.max_fragments
        fragment_lst = []
        prev_obj = supervoxel_ids[0]
        while any(supervoxel_ids):
            cur_obj = supervoxel_ids[0]
            if cur_obj == prev_obj:
                fragment_lst.append(fragments[0])
            else:
                batches.append({'objectId': prev_obj,
                                'fragmentKeys': fragment_lst})
                prev_obj = cur_obj
                fragment_lst = [fragments[0]]
            supervoxel_ids.pop(0)
            fragments.pop(0)
            space_left -= 1
            if space_left == 0:
                break
        # append last entry
        batches.append({'objectId': cur_obj,
                        'fragmentKeys': fragment_lst})
        return batches, supervoxel_ids, fragments

    def _get_mesh_fragment(self, mesh_name, batches):
        """gets list of mesh fragments associated with segment sv_id

        Args:
            mesh_name (str) : name of the mesh collection associated with the
                                segmentation volume
            batches (list) : list of dictionaries for mesh batch
                                       requests:
                                       [{'objectId': supervoxel_id,
                                         'fragmentKeys': [fragmentkey1,
                                                          fragmentkey2,...]
                                        },...
                                       ]

        Returns:
            bytearray containing the information of the triangle mesh
            representation
        """
        url = self.base_url + '/objects/meshes:batch'
        req_body = {
            'volumeId': self.volume_id,
            'meshName': mesh_name,
            'batches': batches
        }
        resp = self.post_request(url, req_body)
        if not any(resp.content):
            raise EmptyResponse(
                'The API response is empty. Check input variables')
        return bytearray(resp.content)

    def _get_mesh_fragment(self, mesh_name, batches):
        """gets list of mesh fragments associated with segment sv_id

        Args:
            mesh_name (str) : name of the mesh collection associated with the
                                segmentation volume
            batches (list) : list of dictionaries for mesh batch
                                       requests:
                                       [{'objectId': supervoxel_id,
                                         'fragmentKeys': [fragmentkey1,
                                                          fragmentkey2,...]
                                        },...
                                       ]

        Returns:
            bytearray containing the information of the triangle mesh
            representation
        """
        url = self.base_url + '/objects/meshes:batch'
        req_body = {
            'volumeId': self.volume_id,
            'meshName': mesh_name,
            'batches': batches
        }
        resp = self.post_request(url, req_body)
        if not any(resp.content):
            raise EmptyResponse(
                'The API response is empty. Check input variables')
        return bytearray(resp.content)

    def download_mesh(self, segment_id, change_stack_id=None):
        """Returns the mesh for segment sv_id

        Meshes of one segment are usually split into several fragments. To
        retrieve the mesh of a particular segment one needs to first retrieve
        the name of the mesh collection associated with a segmentation volume.
        From this the list of fragments into which segment sv_id has been split
        can be retrieved in order to download the meshes.

        Args:
            segment_id (int) : segment id
            volume_id (str) : id of the base segmentation volume
            change_stack_id (str) : id of the agglomeration change stack
            service_account (str) : path to the service account json in order to
                                    authenticate through Google OAuth2
            change_stack_id (str, optional) : name of the change stack, if
                                  given entire mesh of the
                                  agglomerated parent will be
                                  downloaded

        Returns:
            vertices (np.array) : all vertices of the mesh in [x,y,z] voxel
                                    coordinates
            indices (np.array) : indices of the mesh
        """
        mesh_name = self._get_mesh_name()[0]
        supervoxel_ids, fragments = self._get_fragment_list(segment_id,
                                                            mesh_name,
                                                            change_stack_id)
        vertices = np.empty((0, 3), int)
        indices = np.empty((0, 3), int)
        data_to_query = True
        while data_to_query:
            n_fragments = min(len(fragments), self.max_fragments)
            batches, supervoxel_ids, fragments = self.make_query_package(
                supervoxel_ids, fragments)
            bytestream = self._get_mesh_fragment(mesh_name, batches)
            for j in range(n_fragments):
                bytestream, ind, vert = self._mesh_from_stream(bytestream)
                indices = np.append(indices, ind+vertices.shape[0], axis=0)
                vertices = np.append(vertices, vert, axis=0)
            if not supervoxel_ids:
                data_to_query = False

        return vertices, indices

    # SKELETONS
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

    def download_skeleton(self, sv_id, mesh_name=None):
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
        if mesh_name is None:
            mesh_name = self._get_mesh_name(mesh_type='LINE_SEGMENTS')[0]
        skel_json = self._fetch_skeleton(sv_id, mesh_name)
        return self._graph_from_skel_json(skel_json)
