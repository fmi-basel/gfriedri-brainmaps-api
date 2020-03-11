import pickle

from collections import deque
from datetime import datetime
from multiprocessing import cpu_count
from queue import Queue, Empty
from requests.exceptions import HTTPError
from statistics import median
from threading import Thread, Event
from timeit import default_timer as timer

from brainmaps_api_fcn.basic_requests import EmptyResponse
from baf_utils.utils import to_key

TIMEOUT = 2


class ThreadWithReturn(Thread):
    """Thread that writes return valus to dictionary, stops time it takes for
    the request and stores it in deque
    """

    def __init__(self, func, arg_queue, results_dict, request_durations, abort,
                 timestamp_queue):
        super(ThreadWithReturn, self).__init__()
        self.func = func
        self.arg_queue = arg_queue
        self.results = results_dict
        self.request_durations = request_durations
        self.abort = abort
        self.timestamp_queue = timestamp_queue
        self.daemon = True
        self.start()

    def run(self):
        """runs the request, stores the response in a dict which is appended to
        the result_queue
        """
        while not self.abort.is_set():
            ts = datetime.now().timestamp()
            try:
                arg = self.arg_queue.get(timeout=TIMEOUT)
            except Empty:
                continue
            key = 'errors'
            start = timer()
            try:
                response = self.func(arg)
                stop = timer()
                key = 'data'
            except EmptyResponse:
                response = 'the response was returned empty'
                stop = timer()
            except HTTPError as httpe:
                response = 'failed with code ' + str(
                    httpe.response.status_code)
                stop = timer()
            except Exception as e:
                response = 'exception raised: {}'.format(e)
                stop = timer()

            self.timestamp_queue.append({to_key(arg): ts})
            self.request_durations.append(stop - start)
            self.results[key].update({to_key(arg): response})


class RateLimitedRequestsThreadPool:
    """
    Thread pool for BrainMapsAPI requests that limits the rate of requests by
    limited access to tasks in a queue.
    If the request function allows batch requests the speed of the last 15
    requests finishing will determine whether to do batch or single requests.
    This serves to compensate for slower request handling when the equivalence
    graph is not cached (onset or moving to different server)

    Alternate approach (Markus): request arguments are added to a queue as
    single items, workers draw single or batches depending on is_slow flag.
    Rate limiting achieved by allowing only a certain number of threads to run
    at a time?

    Attributes:
        data_queue (queue.Queue) : queue for the request arguments
        min_requests (int): minimal number of request over which to average the
                            duration (set to 15)
        request_durations (collections.deque): duration of the last min_requests
                                            requests finished

        batch_size (int): size of therequest arguments
        _queuing_event (threading.Event): event to continue entry of data into
                                          the data queue. When no more data is
                                          available it is set.
        _queuing_interval (float): interval at which data is put to the
                                    data_queue = period/Nrequests
        func: request function
        func_args: list of input arguments to the request function
        max_workers: maximal number of threads
        use_bulk_requests: Flag that determines whether to use bulk requests
                            or not
        max_batch_size: maximal number of items in a bulk request

    """

    def __init__(self, func, func_args, log_file=None, Nrequests=10 ** 4,
                 period=100, use_bulk_requests=True, max_batch_size=50,
                 max_workers=None, verbose=False):
        """
        Args:
            func: request function
            func_args: list of input arguments to the request function
            max_workers: maximal number of threads
            Nrequests: maximal number of requests in the time window given by
                        period
            period: time window in which Nrequests are allowed to be issued
            use_bulk_requests: Flag that determines whether to use bulk requests
                                or not
            max_batch_size: maximal number of items in a bulk request
        """
        # Producer part
        rate = Nrequests / period
        self.func_args = func_args
        # reduce queue size to half the number of requests per sec -> too small?
        self.data_queue = Queue(maxsize=round(rate / 2))
        self.batched_requests = []

        # variables to measure speed of the request to finish
        self.use_bulk_requests = use_bulk_requests
        self.batch_size = 1
        self.max_batch_size = max_batch_size
        self.min_requests = 10
        self.request_durations = deque(maxlen=self.min_requests)
        self._queuing_event = Event()
        self._queuing_interval = 1 / rate

        self.verbose = verbose
        self._request_timestamps = deque()

        self.start_queuing()

        # Threadpool part
        self.results = {'data': {}, 'errors': {}}
        self.func = func
        if max_workers is None:
            self.max_workers = min(32, cpu_count() + 4)
        else:
            self.max_workers = max_workers
        self.workers = []
        self.abort_events = []

        self.log_file = log_file
        self.run_requests()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if any(not ev.is_set() for ev in self.abort_events):
            self.abort()
        if self.log_file:
            with open(self.log_file, 'wb') as file:
                pickle.dump(self.results, file)

    def start_queuing(self):
        """starts the thread that provides """
        Thread(target=self._extend_queue, daemon=True).start()

    def _extend_queue(self):
        """Places the next batch of input arguments in the data queue"""
        while not self._queuing_event.wait(self._queuing_interval):
            if self.use_bulk_requests:
                self.determine_batch_size()

            if len(self.func_args) == 0:
                self._queuing_event.set()
            elif len(self.func_args) < self.batch_size:
                next_item = self.func_args[:]
            else:
                next_item = self.func_args[:self.batch_size]

            if self.batch_size > 1:
                self.batched_requests.append(to_key(next_item))

            self.func_args = self.func_args[self.batch_size:]
            self.data_queue.put(next_item)

        self.check_all_done()

    def determine_batch_size(self, max_dur=1):
        """function that switches size of a bulk request according to request
        duration

        Args:
            max_dur(int, float): maximum duration in seconds allowed for the
                                median request to take before the bulk size is
                                reduced to 1
        """
        self.batch_size = 1
        if len(self.request_durations) == self.min_requests:
            median_duration = median(self.request_durations)
            if median_duration < max_dur:
                self.batch_size = self.max_batch_size

    def run_requests(self):
        """creates a thread pool of workers that start themselves"""
        print('starting requests')
        for n in range(self.max_workers):
            abort = Event()
            self.abort_events.append(abort)
            self.workers.append(
                ThreadWithReturn(func=self.func, arg_queue=self.data_queue,
                                 results_dict=self.results,
                                 request_durations=self.request_durations,
                                 abort=abort,
                                 timestamp_queue=self._request_timestamps)
            )

    def check_all_done(self):
        """waits for all tasks in the data queue to be processed and then issues
        an abort signal to all workers"""
        while not self.data_queue.empty():
            continue
        self.abort()

    def abort(self):
        """sets abort event to stop workers"""
        print('abort was called at ', datetime.now().time())
        for ev in self.abort_events:
            ev.set()

    def return_data(self):
        """blocks main thread until all worker finished, then returns results"""
        while any([worker.is_alive() for worker in self.workers]):
            pass
        self.cleanup_response_data()
        if self.verbose:
            return self.results, self._request_timestamps
        else:
            return self.results

    def cleanup_response_data(self):
        """Creates a dict with key = request argument and value = responses"""
        # todo: verify that this works for all input argument list for the
        #  brainmaps api requests (e.g. check meshes, skeletons)
        data_dict = dict()
        for values in self.results['data'].values():
            data_dict.update(values)
        self.results['data'] = data_dict

        self.results['errors'] = self._flatten_batch_responses(
            self.results['errors'])
        req_time_stamp = dict()
        for item in self._request_timestamps:
            req_time_stamp.update(self._flatten_batch_responses(item))
        self._request_timestamps = req_time_stamp

    def _flatten_batch_responses(self, dict_in):
        """"""
        dict_out = dict()
        for arg, value in dict_in.items():
            if arg in self.batched_requests:
                dict_out.update({single_arg: value for single_arg in arg})
            else:
                dict_out.update({arg: value})
        return dict_out


def run_pool(func, func_args, max_repeat=5, **kwargs):
    """"""
    results = {'data': dict(),
               'errors': dict()}
    time_stamps = dict()
    verbose = False
    if 'verbose' in kwargs.keys():
        verbose = kwargs['verbose']

    while max_repeat > 0 and len(func_args) > 0:
        with RateLimitedRequestsThreadPool(func=func, func_args=func_args,
                                           **kwargs) as obj:
            return_values = obj.return_data()

        max_repeat = max_repeat - 1
        func_args = []

        if verbose:
            time_stamps.update(return_values[1])
            return_values = return_values[0]
        results['data'].update(return_values['data'])

        if any(return_values['errors']):
            func_args = list(return_values['errors'].keys())

    if any(return_values['errors']):
        results['errors'].update(return_values['errors'])

    if verbose:
        return results, time_stamps

    return results

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
