"""Manages the starting/stopping of the DHT nodes using script arguments.
Also exposes raw methods for putting/getting values from the DHT. For
interoperability with KERI, please see :mod:`keridht.kes` and :mod:`keridht.ipl`.
"""
from collections import deque
import logging
import re
from multiprocessing import Queue, Pipe
import threading
from contextlib import contextmanager
from hio.hioing import VersionError
import opendht as dht
from hio.base.doing import DoDoer, Doer, ServerDoer
from hio.base import tyming
from hio.core.tcp import serving

from keri import help
from keri.kering import Versionage
from keri.core.coring import Serials

from keridht.base import EndpointError, MethodError

log = logging.getLogger(__name__)


nodes = {}
"""dict: keys are `int` port numbers. Values are :class:`dht.DhtRunner`
that run nodes on the local machine.
"""
configs = {}
"""dict: keys are `int` port numbers; Values are `dict` of configuration
parameters needed to run the node.
"""

RP_METHOD = b'DHT(?P<major>[0-9a-f])(?P<minor>[0-9a-f])(?P<size>[0-9a-f]{6})(?P<endpoint>[a-z]{2})(?P<method>[A-Z]{3})'
"""bytes: regex pattern for matching the first bytes of a KERI DHT request. The strings
that this regex targets have a length of exactly 16 bytes (a multiple of 4):
- `DHT`: 3
- major: 1
- minor: 1
- size: 6
- endpoint: 2
- method: 3
"""
RX_METHOD = re.compile(RP_METHOD) #compile is faster
"""Compiled regex pattern for detecting the method of a KERI DHT request.
"""
PREFIX_SIZE = 16
"""int: number of bytes used for the request prefix.
"""


def get_node(port, identity: dht.Identity=None, is_bootstrap=False, ipv4:str="", 
             ipv6:str="", config:dht.DhtConfig=None, bootstrap_node=None,
             logfile=None):
    """Gets the node instance at the specified port.

    Args:
        port (int): port to get the node at.
    """
    log.debug(f"get_node called for port {port}")
    global nodes
    if port not in nodes or not nodes[port].isRunning():
        n = dht.DhtRunner()
        if config is not None:
            n.run(id=identity, is_bootstrap=is_bootstrap, port=port, ipv4=ipv4, ipv6=ipv6, config=config)
        else:
            n.run(id=identity, is_bootstrap=is_bootstrap, port=port, ipv4=ipv4, ipv6=ipv6)
        nodes[port] = n

        if logfile:
            n.enableFileLogging(logfile)

        if bootstrap_node is not None:
            bhost, bport = bootstrap_node.split(':')
            n.bootstrap(bhost, bport)

    return nodes[port]


class LookupValues(list):
    """An extension of the `list` class that provides a method for storing
    the values found by the DHT.

    Args:
        identifier (str): unique ID of the thread that requested this lookup.
        key (str): key in the hash table that was requested.
        callback (callable): function from the :class:`MethodResource` to call
            with the result of the DHT request.
    """
    def __init__(self, identifier, key, callback):
        self.identifier = identifier
        self.key = key
        self.callback = callback


    def done_cb(self, done, nodes):
        log.debug(f"Lookup completed for {self.key}; done={done}; nodes={nodes}.")
        if self.callback is None:
            log.error(f"The resource identifier {self.identifier} does not have a callback to "
                        "send the result to.")
            return

        self.callback(self, done, nodes)


    def get_cb(self, value):
        """Get callback for the DHT get method that appends the value found
        in the DHT.
        """
        log.debug(f"Value received from DHT get lookup of {self.key}: {value}")
        self.append(value)


class GetDoer(Doer):
    """Class responsible for getting an object from the DHT for a specific hash.

    .. note:: This doer will attempt to start the node on :meth:`enter` if it
        isn't already running. This will used cached configuration from when
        the parent :class:`DhtDoer` was initialized. However, this doer does
        *not* close/stop the node on :meth:`exit`.

    Args:
        port (int): port of the node to send the request to.
        tock (float): desired time in seconds between runs or until next run,
            non negative, zero means run ASAP.
        maxsize (int): maximum number of items allowed in the queues.

    Attributes:
        node (dht.DhtRunner): DHT node that the query will be sent to.
        key (dht.InfoHash): representing the key to lookup.
        results (list): of result objects obtained from the DHT for the lookup.
    """
    def __init__(self, port, maxsize=500, tock=0, **kwargs):
        super().__init__(tock=tock, **kwargs)
        log.debug(f"GetDoer initialized for port {port} with queue size {maxsize}.")
        self.port = port
        conf = configs.get(self.port)
        self.node = get_node(port, **conf)
        self.queue = deque(maxlen=maxsize)


    def enter(self):
        """Starts the node if it is not already running.
        """
        log.debug("Entering context creator for GetDoer")
        if not self.node.isRunning():
            conf = configs.get(self.port)
            self.node = get_node(self.port, **conf)


    def recur(self, tyme):
        """Performs the actual get request on the node. This is a "run-once" doer.
        """
        super(GetDoer, self).recur(tyme)
        while len(self.queue) > 0:
            log.debug(f"Processing `recur` for GetDoer; queue len={len(self.queue)}.")
            callback, identifier, key = self.queue.popleft()
            result = LookupValues(identifier, key, callback)
            self.node.get(dht.InfoHash.get(key), get_cb=result.get_cb, done_cb=result.done_cb)


class PutDoer(Doer):
    """Class responsible for putting an object into the DHT for a specific hash.

    .. note:: This doer will attempt to start the node on :meth:`enter` if it
        isn't already running. This will used cached configuration from when
        the parent :class:`DhtDoer` was initialized. However, this doer does
        *not* close/stop the node on :meth:`exit`.

    Args:
        port (int): port of the node to send the request to.
        pipes (dict): keys are thread identifiers; values are the :class:`multiprocessing.connection.Connection`
            objects to write the result to when it is ready.
        tock (float): desired time in seconds between runs or until next run,
            non negative, zero means run ASAP.
        maxsize (int): maximum number of items allowed in the queues.

    Attributes:
        node (dht.DhtRunner): DHT node that the put query will be sent to.
        key (dht.InfoHash): representing the key to put the object under.
    """
    def __init__(self, port, tock=0, maxsize=500, **kwargs):
        super().__init__(tock=tock, **kwargs)
        log.debug(f"PutDoer initialized for port {port} with queue size {maxsize}.")
        self.port = port
        conf = configs.get(self.port)
        self.node = get_node(port, **conf)
        self.queue = deque(maxlen=maxsize)


    def enter(self):
        """Starts the node if it is not already running.
        """
        log.debug(f"Entering context creator for PutDoer; node running {self.node.isRunning()}.")
        if not self.node.isRunning():
            conf = configs.get(self.port)
            self.node = get_node(self.port, **conf)


    def recur(self, tyme):
        """Performs the actual get request on the node. This is a "run-once" doer.
        """
        super(PutDoer, self).recur(tyme)
        while len(self.queue) > 0:
            log.debug(f"Processing `recur` for PutDoer; queue len={len(self.queue)}.")
            callback, key, obj = self.queue.popleft()
            self.node.put(dht.InfoHash.get(key), dht.Value(bytes(obj)), done_cb=callback)


class DhtDoer(DoDoer):
    """Node on the DHT used for storing and getting values. It works as follows:

    1. When the doer is created, it creates a reference to a multiprocessing queue
       that it will consume each time it runs.
    2. On :meth:`enter`, if the node is not running, it starts on the configured
       IP addresses and ports.
    3. On :meth:`exit`, it cleans up the node and closes connections.
    4. On :meth:`recur`, it checks to see if there are any requests on the queues;
       if there are, they are processed; otherwise it just continues.

    This class is designed to run forever until it is shutdown via :meth:`shutdown`.

    Args:
        port (int): port on which the node will listen.
        tock (float): desired time in seconds between runs or until next run,
            non negative, zero means run ASAP.
        maxsize (int): maximum number of items allowed in the queues.
        identity (dht.Identity): specifying the certificates and keys that identify
            this node.
        is_bootstrap (bool): if `True`, then this is a bootstrap node; otherwise,
            this node will connect to another for bootstrapping.
        ipv4 (str): IP4 address to bind to.
        ipv6 (str): IP6 address to bind to.
        config (dht.DhtConfig): additional configuration to run the node with.
        logfile (str): path to a log-file to use for the DHT node logging.
        kwargs (dict): additional keyword arguments for the parent :class:`Doer`.

    Attributes:
        node (dht.DhtRunner): instance of the DHT node that this doer interacts
            with.
    """
    def __init__(self, port, tock=0, maxsize=500, identity: dht.Identity=None, 
                 is_bootstrap=False, ipv4:str="", ipv6:str="", config:dht.DhtConfig=None, 
                 bootstrap_node=None, logfile=None, **kwargs):
        global configs
        configs[port] = {
            "identity": identity,
            "is_bootstrap": is_bootstrap,
            "ipv4": ipv4,
            "ipv6": ipv6,
            "config": config,
            "bootstrap_node": bootstrap_node,
            "logfile": logfile
        }
        log.debug(f"DhtDoer initialized with configuration {configs[port]}.")
        self.port = port
        conf = configs.get(self.port)
        self.node = get_node(port, **conf)
        self.getter = GetDoer(port, maxsize=maxsize)
        self.putter = PutDoer(port, maxsize=maxsize)
        self._shutdown = threading.Event()

        doers = [self.getter, self.putter]
        super(DhtDoer, self).__init__(doers=doers, **kwargs)


    @property
    def shutting_down(self):
        """Checks whether this doer has started shutting down already.
        """
        return self._shutdown.is_set()


    def shutdown(self):
        """Sets the shutdown threading event so that the doer no longer processes
        any events in the queue.
        """
        log.debug(f"Setting shutdown event for {self}.")
        self._shutdown.set()


    def enter(self, doers=None, deeds=None):
        """Runs the DHT node on the configured port if it isn't already running.
        """
        log.debug(f"Running enter context for DhtDoer; node running {self.node.isRunning()}.")
        if not self.node.isRunning():
            conf = configs.get(self.port)
            self.node = get_node(self.port, **conf)

        return super(DhtDoer, self).enter(doers)


class Requester(tyming.Tymee):
    """DHT data requester (Contextor) class with TCP Remoter for putting and 
    getting data from the DHT.
    
    Args:
        dhtdoer (DhtDoer): async doer that handles all the get and put
            operations for this process. Each indepndent worker process
            will have a single `DhtDoer` with possible multiple threads
            running instances of this resource object.

    Attributes:
        remoter: is TCP Remoter instance for connection from remote TCP client.
        persistent (bool): True means keep connection open. Otherwise close.
        ims (bytes): pending received bytes on the connection that haven't been processed.
        get_queue (multiprocessing.Queue): that is being processed for all DHT
            `get` operations.
        put_queue (multiprocessing.Queue): that is being processed for all DHT
            `put` operations.
        receiver (multiprocessing.connection.Connection): receiving end of the
            multithreading pipe that the `DhtDoer` uses to communicate results
            back on operations.
    """
    VERSIONS = [Versionage(major=1, minor=0)]
    """list: of :class:`Versionage` with `(major, minor)` version numbers supported by
    this requester's processor.
    """
    MAJOR = 1
    """int: latest major version number.
    """
    MINOR = 0
    """int: latest minor version number.
    """
    METHODS = [b"GET", b"PUT"]
    """list: of supported method keywords in this requester's processor.
    """
    ENDPOINTS = [b"id", b"ip"]
    """list: of `str` supported endpoint names in the requester's processor.
    """
    RESOURCES = None
    """dict: keys are `(endpoint, method)` tuples; values are :class:`MethodResource`
    for parsing request bytes and executing the method.
    """
    def __init__(self, dhtdoer: DhtDoer, remoter, name, persistent=True, **kwa):
        super(Requester, self).__init__(**kwa)
        self.remoter = remoter  # use remoter for both rx and tx
        self.name = name
        self.ims = self.remoter.rxbs
        self.persistent = True if persistent else False
        self.results = deque()
        self.dhtdoer = dhtdoer

        # Local import here to prevent circular imports.
        if Requester.RESOURCES is None:
            from keridht.methods import IdGet, IdPut, IpGet, IpPut
            Requester.RESOURCES = {
                (b"ip", b"GET"): IpGet,
                (b"ip", b"PUT"): IpPut,
                (b"id", b"GET"): IdGet,
                (b"id", b"PUT"): IdPut
            }


    @classmethod
    def unmarshal(cls, method_key, result):
        """Unmarshals a result from bytes into a `dict`.

        Args:
            method_key (tuple): one of the methods in :attr:`RESOURCES`.
            result (bytearray): taken from the TCP buffer.
        """
        R = cls.RESOURCES.get(method_key)
        if hasattr(R, "unmarshal"):
            return getattr(R, "unmarshal")(result)


    def send(self, msg, label=""):
        """Sends message msg and loggers label if any.

        Args:
            msg (bytes): response from DHT method that was executed.
        """
        self.remoter.tx(msg)  # send to remote
        log.debug("%s sent %s:\n%s\n\n", self.remoter, label, bytes(msg))


    def process_results_iter(self):
        """Generator that yields any results from DHT requests that have not
        been sent to the client that requested them yet.
        """
        while self.results:  # iteratively process each cue in cues
            msgs = bytearray()
            r = self.results.popleft()
            log.info("%s got result: %s\n", self.name, r)
            msgs.extend(r["result"])
            yield r["label"], msgs


    @classmethod
    def build_prefix(cls, endpoint, method, size):
        """Builds the 12-byte prefix to use for the given endpoint and method using
        the current version of the requester.
        """
        return f"DHT{cls.MAJOR}{cls.MINOR}{size:06x}{endpoint}{method}".encode("utf8")


    @classmethod
    def sniff(cls, raw):
        """Extracts the KERI DHT protocol version and payload size from the raw
        bytes of the received message.
        """
        match = RX_METHOD.search(raw)
        if not match:
            raise ValueError("Invalid KERI DHT method in raw bytes = {}".format(raw))

        major, minor, size, endpoint, method = match.group("major", "minor", "size", "endpoint", "method")
        version = Versionage(major=int(major, 16), minor=int(minor, 16))
        if version not in cls.VERSIONS:
            raise VersionError(f"Version number {version} is not supported by this DHT request processor.")
        if endpoint not in cls.ENDPOINTS:
            raise EndpointError(f"Endpoint {endpoint} is not supported by this DHT request processor.")
        if method not in cls.METHODS:
            raise MethodError(f"Method {method} is not supported by this DHT request processor.")

        size = int(size, 16)

        return (version, size, endpoint, method)


    def process_single(self, ims):
        """Processes a single byte array request against the DHT.
        """
        version, size, endpoint, method = Requester.sniff(ims)
        methodkey = (endpoint, method)
        resource = self.RESOURCES.get(methodkey)
        if resource is None:
            raise EndpointError(f"Couldn't find the endpoint/method to execute from {methodkey}.")

        R = resource(self.dhtdoer, self)
        R.parse(ims, size, version)
        R.execute()
        del ims[:PREFIX_SIZE+size]


    def process(self, ims=None):
        """Processes the outstanding queries against the DHT in :attr:`ims`.
        """
        if ims is not None:  # needs bytearray not bytes since deletes as processes
            if not isinstance(ims, bytearray):
                ims = bytearray(ims)  # so make bytearray copy
        else:
            ims = self.ims

        while ims:
            try:
                self.process_single(ims=ims)
            except Exception as ex:  # Some other error while validating
                if log.isEnabledFor(logging.DEBUG):
                    log.exception("DHT request msg validation error: %s\n", ex.args[0])
                else:
                    log.error("DHT msg validation error: %s\n", ex.args[0])


class DhtServer(Doer):
    """TCP server for responding to direct client requests for PUT/GET data
    from the DHT.
    """
    def __init__(self, dhtdoer: DhtDoer, server, name,  **kwa):       
        super(DhtServer, self).__init__(**kwa)
        self.name = name
        self.dhtdoer = dhtdoer
        self.server = server
        self.connections = dict()
        self.results = []


    def closeConnection(self, ca):
        """ Close and remove connection given by ca
        """
        #log.debug(f"{self.name}: closing connections for.")
        if ca in self.connections:
            del self.connections[ca]
        if ca in self.server.ixes:  # remoter still there
            self.server.ixes[ca].serviceSends()  # send final bytes to socket
        self.server.removeIx(ca)


    def serviceConnects(self):
        """New connections get Requester added to .connections
        """
        #log.debug(f"{self.name}: servicing new connections for.")
        for ca, ix in list(self.server.ixes.items()):
            if ix.cutoff:
                self.closeConnection(ca)
                continue

            if ca not in self.connections:
                log.debug(f"Adding new connection for {ix}.")
                self.connections[ca] = Requester(self.dhtdoer, remoter=ix, name=ca)

            if ix.timeout > 0.0 and ix.tymer.expired:
                self.closeConnection(ca)


    def serviceQueries(self):
        """Service pending DHT put/get requests.
        """
        #log.debug(f"{self.name}: servicing queries for {len(self.connections)} connections.")
        for ca, requester in self.connections.items():
            if requester.ims:
                log.info("Server %s received:\n%s\n", self.name, requester.ims)
                requester.process()
                for label, msg in requester.process_results_iter():
                    requester.send(msg, label=label)
                    break  # throttle just do one cue at a time

            if not requester.persistent:  # not persistent so close and remove
                ix = self.server.ixes[ca]
                if not ix.txbs:  # wait for outgoing txes to be empty
                    self.closeConnection(ca)


    def wind(self, tymist):
        """
        Inject new ._tymist and any other bundled tymee references
        Update any dependencies on a change in ._tymist:
            starts over itself at new ._tymists time
        """
        super(DhtServer, self).wind(tymist)
        self.server.wind(tymist)


    def enter(self):
        """"""
        self.server.reopen()


    def recur(self, tyme):
        """"""
        self.server.service()
        self.service()


    def exit(self):
        """"""
        self.server.close()


    def service(self):
        """
        Service connects and rants
        """
        self.serviceConnects()
        self.serviceQueries()


@contextmanager
def dhtserver(dhtdoer, host, port):
    log.info(f"Creating TCP server at {host}:{port}.")
    with serving.openServer(cls=serving.Server, host=host, port=port) as server:
        yield DhtServer(dhtdoer, server, f"{host}:{port}")