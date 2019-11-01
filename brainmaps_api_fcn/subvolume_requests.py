import numpy as np
try:
    import snappy
    snappy_ = True
except ImportError:
    snappy_ = False


from brainmaps_api_fcn.basic_requests import BrainMapsRequest


class SubvolumeRequest(BrainMapsRequest):
    """Retrieves image or segmentation data (segment Ids) in a subvolume


    suggested upper bound ~256^3 voxels (max. 1GB)

    Attributes:
        volume_id (str) : id of the base segmentation volume
        base_url (str) : base url of API functions
    """

    def __init__(self, service_account_secrets, volume_id, **kwargs):
        """sets get and post request functions (see class AutherticateCall)

        Args:
            volume_id (str) : id of the base segmentation volume
            service_account_secrets (str) : path to the service account json in
                                            order to authenticate through Google
                                            OAuth2
            change_stack_id (str) : id of the agglomeration change stack
        """
        super(SubvolumeRequest, self).__init__(volume_id=volume_id,
                                               service_account_secrets=service_account_secrets,
                                               **kwargs)

    def get_subvolume(self, corner, size, volume_datatype=np.uint64):
        """Downloads subvolume of image data

        Args:
            corner (list) : [x,y,z] coordinates of the upper left corner of the
                            subvolume to retrieve
            size (list) : [x,y,z] dimensions of the subvolume to retrieve
            volume_datatype : np.uint64 for segmentation data, np.uint8 for raw
                            image data

        Returns:
            array (np.array) : image data
        """
        url = self.base_url + '/volumes/{volume_id}/subvolume:binary'.format(
            volume_id=self.volume_id)
        sv_format = 'RAW'
        if snappy_:
            sv_format = 'RAW_SNAPPY'
        body = {
            'geometry': {
                'scale': 0,
                'corner': ','.join(str(x) for x in corner),
                'size': ','.join(str(x) for x in size),
            },
            'subvolumeFormat': sv_format,
        }
        resp = self.post_request(url, body)
        if snappy_:
            data = snappy.decompress(resp.content)
        else:
            data = resp.content
        array = np.frombuffer(data, dtype=volume_datatype).reshape(size)
        return array
