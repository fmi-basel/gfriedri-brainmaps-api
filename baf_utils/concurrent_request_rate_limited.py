from collections import deque
from multiprocessing import cpu_count
from queue import Queue
from requests.exceptions import HTTPError
from statistics import median
from threading import Thread, Event
from timeit import default_timer as timer

from brainmaps_api_fcn.basic_requests import EmptyResponse
from baf_utils.utils import to_key

_sentinel = object()


class ThreadWithReturn(Thread):
    """"""
    def __init__(self, func, arg_queue, result_queue, request_durations, abort):
        super(ThreadWithReturn, self).__init__()
        self.func = func
        self.arg_queue = arg_queue
        self.result_queue = result_queue
        self.request_durations = request_durations
        self.abort = abort
        self.daemon = True
        self.start()

    def run(self):
        """runs the request, stores the response in a dict which is appended to
        the result_queue
        """
        while not self.abort.is_set():
            start = timer()
            arg = self.arg_queue.get()
            result = dict()
            key = 'errors'
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

            self.request_durations.append(stop - start)
            result[key] = {to_key(arg): response}
            self.result_queue.put(result)
        print('thread.run is not in while loop')


class RateLimitedRequestsThreadPool:
    """
    Attributes:
        request_durations (collections.deque): duration of the last x requests
                                finished
        min_requests (int): minimal number of request over which to average the
                    duration
    """

    def __init__(self, func, func_args, Nrequests=10 ** 4, period=100,
                 use_bulk_requests=True, max_batch_size=50, max_workers=None):
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

        # variables to measure speed of the request to finish
        self.use_bulk_requests = use_bulk_requests
        self.batch_size = 1
        self.max_batch_size = max_batch_size
        self.min_requests = 15
        self.request_durations = deque(maxlen=self.min_requests)
        self._queuing_event = Event()
        self._queuing_interval = 1 / rate

        self.start_queuing()

        # Threadpool part
        # variables for concurrent request
        self.func = func
        if max_workers is None:
            self.max_workers = min(32, cpu_count() + 4)
        else:
            self.max_workers = max_workers
        self.workers = []
        self.result_queue = Queue()
        self.abort = Event()
        self.run_requests()

        self.results = {'data': {}, 'errors': {}}
        self.cleanup_response_thread = Thread(
            target=self.cleanup_result_queue(),
            daemon=True)
        self.cleanup_response_thread.start()

    def start_queuing(self):
        """"""
        self.queuing_thread = Thread(target=self._extend_queue, daemon=True)
        self.queuing_thread.start()

    def _extend_queue(self):
        """Places the next batch of input arguments in the data queue"""
        while not self._queuing_event.wait(self._queuing_interval):
            if self.use_bulk_requests:
                self.determine_batch_size()

            if len(self.func_args) == 0:
                # function argument iterable is empty: place sentinel in queue
                # to stop threadpool and terminate producer thread
                next_item = _sentinel
                self._queuing_event.set()
                print('queuing event was set')
            elif len(self.func_args) < self.batch_size:
                next_item = self.func_args[:]
            else:
                next_item = self.func_args[:self.batch_size]
            self.func_args = self.func_args[self.batch_size:]
            self.data_queue.put(next_item)
        # set abort event upon exit of the loop
        self.abort.set()
        print('abort event was set')

    def determine_batch_size(self, max_dur=1):
        """function that switches size of a bulk request according to request
        duration

        Args:


            max_bulk_size (int): maximum size of a bulk_request
            max_dur(int, float): maximum allowed duration in seconds for the mean
                                request to take before the bulk size is reduced to 1

        Returns:
            int: current bulk size for a request
        """
        self.batch_size = 1
        if len(self.request_durations) == self.min_requests:
            median_duration = median(self.request_durations)
            if median_duration < max_dur:
                self.batch_size = self.max_batch_size

    def run_requests(self):
        """creates a thread pool of workers that start themselves"""
        for n in range(self.max_workers):
            self.workers.append(
                ThreadWithReturn(func=self.func, arg_queue=self.data_queue,
                                 result_queue=self.result_queue,
                                 request_durations=self.request_durations,
                                 abort=self.abort)
            )

    def cleanup_result_queue(self):
        """"""
        # todo: check why while condition is always True
        # get results while workers are active
        while any([worker.is_alive() for worker in self.workers]):
            # print('results are being cleaned up')
            if not self.result_queue.empty():
                self.get_results()
            else:
                continue
        # could there be still items in the result queue when the while condition becomes false???
        while not self.result_queue.empty():
            self.get_results()

        print('Finished! All responses stored in results attribute')

    def get_results(self):
        response = self.result_queue.get()
        key = next(iter(response.keys()))
        self.results[key].update(response[key])