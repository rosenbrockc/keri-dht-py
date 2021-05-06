from collections import deque
import threading
import logging
import random
from time import time
from os import urandom

from hio.base import doing

from keridht.methods import METHODS
from keridht.client import DhtTcpClient

log = logging.getLogger(__name__)

RANDOM_SEED = 42
"""int: seed for seeding the python random functions.
"""
random.seed(RANDOM_SEED)


def random_payload():
    """Generates a random payload of bytes of a random length to place
    in the DHT.

    Returns:
        bytes: of random length representing an arbtirary payload.
    """
    # Pick a random number of bytes between 32 and 32K TODO Add 32K instead of 64
    size = random.randint(32, 64)
    return urandom(size)


def random_key(index):
    """Generates a pseudo-random key based on the specified index so that
    it can be done deterministically.

    Args:
        index (int): integer index of the testing for which a key is being
            returned.
    """
    R = random.randint(0, 1e12)
    return f"kdht-{R}-{index}"


class TestClient(doing.DoDoer):
    """Represents a collection of clients that submit requests to the DHT server
    endpoints on the `hio` scheduler.

    Args:
        name (str): name to assign to this client collection for log messages.
        clients (list): of :class:`clienting.Client` to process TCP send/receive.
        maxtests (int): maximum number of `put` tests to perform.
        kwargs (dict): of additional arguments to pass to the :class:`doing.DoDoer`.

    Attributes:
        keys (set): of unique strings used for getting/putting from the DHT.
        gets (deque): of outstanding get requests. Note that when an item is successfully
            `put` in the DHT, it gets added to `gets` with a random integer indicating the
            number of times it should be gotten in the future. This number gets decremented
            each time the get for that key is performed.
        puts (deque): outstanding put request data for clients to submit "simultaneously"
            to the TCP server.
        pending (list): that has the same indices as the clients in :attr:`clients`. Has `set`s
            of result keys that haven't completed yet in the client.
        test_count (int): current number of `put` tests that have been scheduled.
        completed (set): of `int` client indices that have completed all of their pending 
            results.
        timing (dict): keys are the DHT `key`; values are `list` of `(op, time)`, 
            where `time` is the time at which the operation was queued with the client 
            until the result was received from the remote server; `op` is the operation 
            type (GET or PUT) with client identifier `op:client_id`.
        data (dict): keys are the DHT `key`; values are `bytes` that were randomly
            generated as the payload. Used for data integrity verification of GET
            methods after a PUT.
    """
    PUT_METHODS = ["put_id_sync", "put_ip_sync"]
    """list: of `str` method names on :class:`DhtTcpClient` that can be used to put data
    into the DHT.
    """
    GET_METHODS = {
        "id": "get_id_sync", 
        "ip": "get_ip_sync"
    }
    """list: of `str` method names on :class:`DhtTcpClient` that can be used to get data
    from the DHT.
    """

    def __init__(self, name, clients, maxtests=1000, doers=None, **kwargs):
        self.name = name
        self.clients = clients

        self.keys = set()
        self.gets = deque()
        self.puts = deque()
        self.pending = [set() for i in range(len(self.clients))]
        self.test_count = 0
        self.maxtests = maxtests
        self.completed = set()

        self.timing = {}
        self.data = {}

        doers = doers if doers is not None else []
        doers.extend([self.client_puts, self.client_gets, self.test_scheduler, self.result_checker])
        super(TestClient, self).__init__(doers=doers, **kwargs)

        self._shutting_down = threading.Event()


    def shutdown(self):
        """Sets the thread-safe shutdown flag to prevent further processing.
        """
        self._shutting_down.set()


    @doing.doize()
    def test_scheduler(self, tymist=None, tock=0.25, **opts):
        """ Returns Doist compatibile generator method (doer dog) to add new
        DHT put operations to :attr:`puts` so that they will be processed by
        the scheduler and :meth:`client_puts`. Note that once a put request
        completes, the :meth:`result_checker` will then queue the appropriate
        get requests.
        """
        while True and not self._shutting_down.is_set():
            if self.test_count >= self.maxtests:
                yield
                continue

            # See how many to schedule this round; don't want to do too many or the server
            # and client doers won't get to process any tx/rx.
            process_count = 1#random.randint(1, 2)
            client_ids = random.choices(range(len(self.clients)), k=process_count)

            for i in client_ids:
                key = random_key(self.test_count)
                # Put methods take arguments of (key, payload)
                payload = random_payload()
                # Store the data for GET verification later on.
                self.data[key] = payload
                r = {
                    "client_id": i, 
                    "method": random.choice(TestClient.PUT_METHODS),
                    "args": (key, payload),
                    "key": key
                }

                self.puts.append(r)
                self.test_count += 1
                self.timing[key] = [(f"{METHODS.PUT.name}:{i}:o", time())]

            yield

        log.info(f"Finished scheduling {self.test_count}/{self.maxtests} PUT requests.")
        return True


    def _queue_random_get(self, endpoint, key, result):
        """Queues a random number of gets to be executed in the future for a
        given key.
        """
        if result["done"] == True and len(result["nodes"]) > 0:
            counts = random.randint(0, 5)
            client_id = random.randint(0, len(self.clients)-1)
            log.debug(f"Queueing random GET operation for {endpoint} and {key}; client={client_id}")
            r = {
                "client_id": client_id,
                "method": TestClient.GET_METHODS[endpoint],
                "args": (key,),
                "key" : key,
                "counts": counts
            }
            self.gets.append(r)
            self.timing[key].append((f"{METHODS.GET.name}:{client_id}:o", time()))

        else:
            log.info(f"PUT result {result} is not done or doesn't have any nodes.")


    def _verify_integrity(self, key, data):
        """Verifies that the data for `key` matches what was originally PUT
        into the DHT.
        """
        # In testing mode, we only ever have a single value for a key in the DHT.
        assert self.data[key] == data[0]


    @doing.doize()
    def result_checker(self, tymist=None, tock=0.0, **opts):
        """ Returns Doist compatibile generator method (doer dog) to process
        the :attr:`DhtTcpClient.results` dictionary to see if any new results
        have become available.
        """
        while True and not self._shutting_down.is_set() and len(self.completed) < len(self.clients):
            for i, client in enumerate(self.clients):
                for result_key in self.pending[i].copy():
                    if result_key in client.results:
                        result = client.results.pop(result_key)
                        endpoint, _, key = DhtTcpClient.parse_result_key(result_key)

                        if result["method"] == METHODS.PUT.name:
                            log.debug(f"{self.name}: received put result {result} for {result_key}.")
                            self._queue_random_get(endpoint, key, result)
                        else:
                            self._verify_integrity(key, result["result"])

                        self.pending[i].remove(result_key)
                        self.timing[key].append((f"{result['method']}:{i}:i", time()))

                if self.test_count == self.maxtests:
                    # We want to check to see if any of the clients have finished
                    # all of their pending operations.
                    if len(client.results) == 0 and len(self.pending[i]) == 0:
                        log.info(f"Client #{i} has completed all pending tasks.")
                        self.completed.add(i)

            yield

        # Once we are done, we can stop the other generators from computing.
        self.shutdown()
        return True


    @doing.doize()
    def client_gets(self, tymist=None, tock=0.0, **opts):
        """ Returns Doist compatibile generator method (doer dog) to process
            :attr:`pending` deque.

        Usage:
            Add to doers list.
        """
        while True and not self._shutting_down.is_set():
            for client_id, method, args, key in self.process_gets_iter():
                self.get(client_id, method, args, key)

            yield

        return True


    @doing.doize()
    def client_puts(self, tymist=None, tock=0.0, **opts):
        """ Returns Doist compatibile generator method (doer dog) to process
            :attr:`pending` deque.

        Usage:
            Add to doers list.
        """
        while True and not self._shutting_down.is_set():
            for client_id, method, args, key in self.process_puts_iter():
                self.put(client_id, method, args, key)

            yield

        return True


    def get(self, client_id, method, args, key):
        """Processes a get request using the given client.

        Args:
            client_id (int): index of the client to use for the request.
            method (str): class method on the :class:`DhtTcpClient` class to execute.
            args (tuple): of arguments to pass to `method`.
            key (str): unique key being addressed by this request.
        """
        log.debug(f"Processing client #{client_id} get for {method} and {key}.")
        c = self.clients[client_id]
        if hasattr(c, method):
            result_key = getattr(c, method)(*args)
            self.pending[client_id].add(result_key)
        else:
            log.warning(f"Method {method} does not exist on client #{client_id}.")


    def put(self, client_id, method, args, key):
        """Processes a put request using the given client.

        Args:
            client_id (int): index of the client to use for the request.
            method (str): class method on the :class:`DhtTcpClient` class to execute.
            args (tuple): of arguments to pass to `method`.
            key (str): unique key being addressed by this request.
        """
        log.debug(f"Processing client #{client_id} get for {method} and {key}.")
        c = self.clients[client_id]
        if hasattr(c, method):
            result_key = getattr(c, method)(*args)
            self.pending[client_id].add(result_key)
        else:
            log.warning(f"Method {method} does not exist on client #{client_id}.")


    def process_gets_iter(self):
        """Generator that yields any results from DHT requests that have not
        been sent to the client that requested them yet.
        """
        while len(self.gets) > 0 and not self._shutting_down.is_set():
            r = self.gets.popleft()
            log.info("%s got GET request: %s\n", self.name, r)
            # client_id, method, args, key
            yield r["client_id"], r["method"], r["args"], r["key"]

            # If it needs to be gotten again, decrement the counter and re
            # append it to the deque.
            if r["counts"] > 1:
                r["counts"] = r["counts"] - 1
                client_id = random.randint(0, len(self.clients)-1)
                r["client_id"] = client_id
                self.gets.append(r)
                self.timing[r["key"]].append((f"{METHODS.GET.name}:{client_id}:o", time()))


    def process_puts_iter(self):
        """Generator that yields outstanding put request data that should be
        submitted by one of the clients.
        """
        while len(self.puts) > 0 and not self._shutting_down.is_set():
            r = self.puts.popleft()
            log.info("%s got PUT request: %s\n", self.name, r)
            # client_id, method, args, key
            yield r["client_id"], r["method"], r["args"], r["key"]