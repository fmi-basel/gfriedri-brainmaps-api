from collections import deque
from queue import Queue
from threading import Thread, Event
from multiprocessing import cpu_count
from timeit import default_timer as timer
from datetime import timedelta

sentinel = object()


class ThreadWithReturn(Thread):
    """"""

    def __init_(self, func, arg_queue, result_queue, request_durations, abort):
        super.__init__()
        self.func = func
        self.arg_queue = arg_queue
        self.result_queue = result_queue
        self.request_durations = request_durations
        self.abort = abort
        self.daemon = True

    def run(self):
        """"""
        # todo custom thread that 1.add result to queue, 2.handle exceptions
        while not self.abort.is_set():
            start = timer()
            result = self.func(self.arg_queue.get())
            stop = timer()
            self.request_durations.append(timedelta(seconds=stop - start))


# todo: 1. process/return results 2. kill pool when done 3. function to call this 4....
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
        self.data_queue = Queue(max_size=round(rate / 2))

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
        self.result_queue = Queue()
        self.abort = Event()

    def start_queuing(self):
        """"""
        Thread(target=self.extend_queue, daemon=True).start()

    def _extend_queue(self):
        """Places the next batch of """
        while not self._queuing_event.wait(self._queuing_interval):
            if self.use_bulk_requests:
                self.determine_batch_size()

            if len(self.func_args) == 0:
                # function argument iterable is empty: place sentinel in queue
                # to stop threadpool and terminate producer thread
                next_item = sentinel
                self._queuing_event.set()
            elif len(self.args) < self.batch_size:
                next_item = self.func_args[:]
            else:
                next_item = self.func_args[:self.batch_size]
            self.func_args = self.func_args[self.batch_size:]
            self.data_queue.put(next_item)

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
            # todo: switch to median duration
            mean_duration = sum(self.request_durations) / len(
                self.request_durations)
            if mean_duration < max_dur:
                self.batch_size = self.max_batch_size

    def run_requests(self):
        """"""
        for n in self.max_workers:
            ThreadWithReturn(func=self.func, input_queue=self.data_queue,
                             result_queue=self.result_queue, abort=self.abort)
