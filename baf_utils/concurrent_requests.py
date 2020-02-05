import pickle
from brainmaps_api_fcn.basic_requests import EmptyResponse
from requests.exceptions import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Iterable, Hashable


# adapted from https://stackoverflow.com/questions/27049998/convert-a-mixed-nested-list-to-a-nested-tuple
def to_tuple(lst):
    return tuple(to_tuple(i) if isinstance(i, Iterable) else i for i in lst)


def to_key(item):
    """Converts request input argument in a hashable type in order to use it
    as a key"""
    if isinstance(item, Hashable):
        return item
    elif isinstance(item, Iterable):
        return to_tuple(item)
    else:
        raise RuntimeError('This is a situation that should not occur. But '
                           'Markus warned me that it would, recommending '
                           'putting up an error message. So seeing this just '
                           'proves him right again!')


class RunConcurrentRequest:
    """Class to run BrainMapsAPI requests concurrently using a
    ThreadPoolExecutor

    Should be used as a context manager. Returns responses as a dictionary (see
    run_request) and optionally stores a logfile in form of a pickle.

    Attributes:
        args (list): list of arguments that are passed to the request function
        response_data (dict): stores response for all items passed to the
                              request_fcn in nested dict for successful and
                              failed requests
        log_file (None, str): file name for the log file
        request_fcn: function to run the request
        unpack (boolean): flag which determines whether the arguments in args
                          should be unpacked before passing to the request
                          function

    """
    def __init__(self, request_fcn, args, log_file=None, unpack=False):
        """initiates RunConcurrentRequests

        Args:
            request_fcn: function to run the request with
            args (list): list of arguments that are passed to the request
                         function
            log_file (str, optional): full file name to store the responses in
                                      a log file (pickle)
            unpack (boolean, optional): flag which decides whether the arguments
                                        in args should be unpacked before being
                                        passed to the request function
        """
        self.args = args
        self.response_data = {'data': dict(),
                              'errors': dict()}
        self.log_file = log_file
        self.request_fcn = request_fcn
        self.unpack = unpack

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.log_file:
            with open(self.log_file, 'wb') as file:
                pickle.dump(self.response_data, file)

    def run_request(self):
        """function to run requests concurrently

        Returns:
             dict: dictionary with 2 keys:
                'data' (dict):  key: argument from args,
                                value: response of successful requests
                'errors'(dict): keys: argument from args,
                                value: error code of failed requests
        """
        with ThreadPoolExecutor() as executor:
            if self.unpack:
                future_iterable = {
                    executor.submit(lambda p: self.request_fcn(*p), item): item
                    for item in self.args}
            else:
                future_iterable = {
                    executor.submit(self.request_fcn, item): item for item in
                    self.args}
            for future in as_completed(future_iterable):
                item = future_iterable[future]
                key = to_key(item)
                try:
                    return_val = future.result()
                    self.response_data['data'][key] = return_val
                except EmptyResponse:
                    self.response_data['errors'][
                        key] = 'the response was returned empty'

                except HTTPError as httpe:  # some kind of HttpError - store number....
                    print('this threw an error', key)
                    self.response_data['errors'][
                        key] = 'failed with code ' + str(
                        httpe.response.status_code)
        return self.response_data


# BrainMapsAPI requests to run this
# set_equivalence: arg = edge, out = group_id,
#                  RunConcurrentRequest input: list of edges
# delete_equivalence: arg = edge, out = Http response,
#                     RunConcurrentRequest input: list of edges - better use multidelete!
# get_list: arg = segment, out = list of_edges.,
#           RunConcurrentRequest input: list of segments
# get_groups: arg = segment, out = list of segment,
#             RunConcurrentRequest input: list of segments
# get_maps: arg = segment, out = segment,
#           RunConcurrentRequest input: list of segments
# get_subvolume: args = corner, size, volume_datatype=np.uint64,
#                out = array,
#                RunConcurrentRequest input: list of corner size and data_type
#                lists or tuples
# download_skeleton: args = segment, out = nx graph,
#                    RunConcurrentRequest input: list of segments
# download mesh - for now can work with: args = segment, out = 2 arrays
#                                        RunConcurrentRequest input: list of
#                                        segments
