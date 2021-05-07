import threading
import logging
from collections import deque
from contextlib import contextmanager
from typing import Union

from keri.core.coring import Signer, Verfer

from hio.base import doing
from hio.core.tcp import clienting
import asyncio

from keridht.node import Requester
from keri.core.eventing import Serder

from .methods import DhtGetterResource, DhtPutterResouce, IdGet, IdPut, IpGet, IpPut
from .app import TOCK
from .node import PREFIX_SIZE

log = logging.getLogger(__name__)


class ClientResultError(Exception):
    """Represents an error arising from trying to parse the result from 
    a request to the DHT server.
    """


class GenericClient(doing.DoDoer):
    """Represents an abstract client class that can query the DHT with
    either GET/PUT methods.

    .. note:: The :meth:`process_single` *must* be overridden by the subclass
        of this client.

    Args:
        name (str): name to assign to this client for log messages.
        client (clienting.Client): to process TCP send/receive.
        kwargs (dict): of additional arguments to pass to the :class:`doing.DoDoer`.
    """
    def __init__(self, name, client: clienting.Client, doers=None, **kwargs):
        self.name = name
        self.client = client
        self.pending = deque()

        client_doer = doing.ClientDoer(client)
        doers = doers if doers is not None else []
        doers.extend([client_doer, self.receive_do, self.send_do])

        super(GenericClient, self).__init__(doers=doers, **kwargs)

        self._shutting_down = threading.Event()


    def shutdown(self):
        """Sets the thread-safe shutdown flag to prevent further processing.
        """
        self._shutting_down.set()


    def enter(self, doers=None, deeds=None):
        self.client.reopen()
        return super(GenericClient, self).enter(doers=doers, deeds=deeds)


    @doing.doize()
    def receive_do(self, tymist=None, tock=0.0, **opts):
        """Returns Doist compatibile generator method (doer dog) to process
            incoming message stream of the client.
            
        Usage:
            Add to doers list.
        """
        if self._shutting_down.is_set():
            return True

        if self.client.rxbs:
            log.info("Client %s received:\n%s\n...\n", self.name, self.client.rxbs[:1024])
        done = yield from self.processor()  # process messages continuously
        return done  # should never get here except forced close


    @doing.doize()
    def send_do(self, tymist=None, tock=0.0, **opts):
        """ Returns Doist compatibile generator method (doer dog) to process
            :attr:`pending` deque.

        Usage:
            Add to doers list.
        """
        while True and not self._shutting_down.is_set():
            for label, msg in self.process_requests_iter():
                self.send(msg, label=label)
                break # throttle so that only one gets processed per scheduler iteration.
            yield


    def send(self, msg, label=""):
        """Sends message msg and loggers label if any.

        Args:
            msg (bytes): response from DHT method that was executed.
        """
        if not self.client.connected or not self.client.accepted:
            log.warning("The client is trying to send bytes but it isn't connected\n"
                        f"connected={self.client.connected}; accepted={self.client.accepted}\n"
                        f"host={self.client.host}:{self.client.port}.")
            self.client.reopen()

        self.client.tx(msg)
        log.debug("TCP sent %s:\n%s\n", label, bytes(msg))


    def process_requests_iter(self):
        """Generator that yields any results from DHT requests that have not
        been sent to the client that requested them yet.
        """
        while len(self.pending) > 0 and not self._shutting_down.is_set():
            # iteratively process each pending request.
            msgs = bytearray()
            r = self.pending.popleft()
            log.info("%s got request: %s\n", self.name, r)
            msgs.extend(r["request"])
            yield r["label"], msgs


    def process_single(ims=None):
        """Generator that continually processes messages from the bytearray `ims`.

        .. note:: We can use a single client object to process all kinds of requests
            to the DHT servers. As such, we choose here to raise a NotImplementedError
            rather than use abstract classes.
        """
        raise NotImplementedError("The process_single method *must* be overidden by subclass.")


    def processor(self, ims=None):
        """ Returns generator to continually process messages from incoming message
        stream, `ims`. Yields waits whenever `ims` empty. If ims not provided then process 
        messages from :attr:`ims`.

        Parameters:
            ims (bytearray): incoming message stream. May contain one or more
                sets each of a serialized message with attached cryptographic
                material such as signatures or receipts.
        """
        if ims is not None:  # needs bytearray not bytes since deletes as processes
            if not isinstance(ims, bytearray):
                ims = bytearray(ims)  # so make bytearray copy
        else:
            ims = self.client.rxbs  # use instance attribute by default

        while True and not self._shutting_down.is_set():  # continuous stream processing
            try:
                done = yield from self.process_single(ims=ims)

            except ClientResultError as ex:
                if log.isEnabledFor(logging.DEBUG):
                    log.exception("Client result parsing error: %s\n", ex.args[0])
                else:
                    log.error("Client result parsing error: %s\n", ex.args[0])
            yield

        return True


class DhtTcpClient(GenericClient):
    """Client object that can interact with the KDHT server endpoints to retrieve
    and put objects from/to the DHT.

    Args:
        name (str): name to assign to this client for log messages.
        client (clienting.Client): to process TCP send/receive.
        kwargs (dict): of additional arguments to pass to the :class:`doing.DoDoer`.

    Attributes:
        results (dict): keys are `endpoint/method/key`; values are the results
            of the asynchronous calls.
    """
    def __init__(self, name, client, doers=None, **kwargs):
        super(DhtTcpClient, self).__init__(name, client, doers=doers, **kwargs)
        self.results = {}


    @classmethod
    def _get_results_key(cls, endpoint, method, key):
        """Returns the key under which a result will be added to :attr:`keys`.
        """
        return f"{endpoint}/{method}/{key}"


    @classmethod
    def parse_result_key(cls, result_key):
        """Parses the result key to extract its component parts.

        Returns:
            tuple: of `(endpoint, method, key)` from the result.
        """
        return result_key.split('/')


    def process_single(self, ims):
        """Generator that continually processes messages from the bytearray `ims`.
        """
        if len(ims) > 0:
            log.debug(f"Processing single message of size {len(ims)}.")
            # We basically just parse out the endpoint name, method and key and
            # add the rest of the byte payload as the actual result.
            version, size, endpoint, method = Requester.sniff(ims)
            log.debug(f"Message has size {size} for {endpoint}/{method}.")

            data = ims[PREFIX_SIZE:PREFIX_SIZE+size]
            R = Requester.unmarshal((endpoint, method), data)
            R["endpoint"] = endpoint.decode("utf8")
            R["method"] = method.decode("utf8")

            result_key = DhtTcpClient._get_results_key(R["endpoint"], R["method"], R["key"])
            self.results[result_key] = R

            log.info(f"Processed client result is {R}")

            del ims[:PREFIX_SIZE+size]

        yield False


    def _generic_request_sync(self, cls: Union[DhtGetterResource, DhtPutterResouce], 
                              key: str, *args):
        """Schedules a generic GET/PUT request for any subclass of the `Dht*Resource`

        Args:
            cls: the subclass of `Dht*Resource` to build the request for.
            key (str): which key in the DHT will be affected.
            args (tuple): of additional arguments to pass to the respective :meth:`build_request`.
            timeout (float): maximum number of seconds to wait for the result.
        """
        log.debug(f"Building request for {key} and class {cls}.")
        msg = cls.build_request(key, *args)
        name = cls.__name__.lower()
        endpoint, method = name[0:2], name[2:].upper()
        label = f"{endpoint}/{method}"

        rqmsg = {
            "request": msg,
            "label": label
        }
        log.debug(f"Appending {rqmsg} to the client pending queue.")
        self.pending.append(rqmsg)

        result_key = DhtTcpClient._get_results_key(endpoint, method, key)
        return result_key


    async def _generic_request(self, cls: Union[DhtGetterResource, DhtPutterResouce], 
                               key: str, *args, timeout=120):
        """Schedules a generic GET/PUT request for any subclass of the `Dht*Resource`

        Args:
            cls: the subclass of `Dht*Resource` to build the request for.
            key (str): which key in the DHT will be affected.
            args (tuple): of additional arguments to pass to the respective :meth:`build_request`.
            timeout (float): maximum number of seconds to wait for the result.
        """
        result_key = self._generic_request_sync(cls, key, *args)
        total = 0
        while result_key not in self.results and total < timeout:
            log.debug(f"Waiting for {result_key} to become available after {total}/{timeout} seconds.")
            await asyncio.sleep(TOCK)
            total += TOCK

        return self.results.pop(result_key, None)


    async def get_id(self, key, timeout=120):
        """Retrieves the object with `key` from the DHT via a request
        to a KDHT server.

        Args:
            key (str): which KES in the DHT will be retrieved.
            timeout (float): maximum number of seconds to wait for the result.
        """
        return await self._generic_request(IdGet, key, timeout=timeout)


    async def get_ip(self, key, timeout=120):
        """Retrieves the object with `key` from the DHT via a request
        to a KDHT server.

        Args:
            key (str): which witness IP information in the DHT will be retrieved.
            timeout (float): maximum number of seconds to wait for the result.
        """
        return await self._generic_request(IpGet, key, timeout=timeout)


    async def put_id(self, key, payload, timeout=60):
        """Puts the object with `key into the DHT via a request
        to a KDHT server.

        Args:
            key (str): key for the KES that is being put into the DHT.
            timeout (float): maximum number of seconds to wait for the operation.
        """
        return await self._generic_request(IdPut, key, payload, timeout=timeout)


    async def put_ip(self, key, payload, timeout=60):
        """Puts the witness IP information `payload` with `key into the DHT via a request
        to a KDHT server.

        Args:
            key (str): key for the witness IP information that is being put into the DHT.
            timeout (float): maximum number of seconds to wait for the operation.
        """
        return await self._generic_request(IpPut, key, payload, timeout=timeout)


    def get_id_sync(self, key):
        """Retrieves the object with `key` from the DHT via a request
        to a KDHT server.

        .. note:: This method only *queues* the request and returns immediately.
            You are expected to process the :attr:`results` queue separately using
            whatever asynchronous method you prefer.

        Args:
            key (str): which KES in the DHT will be retrieved.

        Returns:
            str: the result key that the result will be available under in :attr:`results`.
        """
        return self._generic_request_sync(IdGet, key)


    def get_ip_sync(self, key):
        """Retrieves the object with `key` from the DHT via a request
        to a KDHT server.

        .. note:: This method only *queues* the request and returns immediately.
            You are expected to process the :attr:`results` queue separately using
            whatever asynchronous method you prefer.

        Args:
            key (str): which witness IP information in the DHT will be retrieved.

        Returns:
            str: the result key that the result will be available under in :attr:`results`.
        """
        return self._generic_request_sync(IpGet, key)


    def put_id_sync(self, key, payload):
        """Puts the object with `key into the DHT via a request
        to a KDHT server.

        .. note:: This method only *queues* the request and returns immediately.
            You are expected to process the :attr:`results` queue separately using
            whatever asynchronous method you prefer.

        Args:
            key (str): key for the KES that is being put into the DHT.
            payload (bytes): the latest KES state.

        Returns:
            str: the result key that the result will be available under in :attr:`results`.
        """
        return self._generic_request_sync(IdPut, key, payload)


    def put_ip_sync(self, key, payload):
        """Puts the witness IP information `payload` with `key into the DHT via a request
        to a KDHT server.

        .. note:: This method only *queues* the request and returns immediately.
            You are expected to process the :attr:`results` queue separately using
            whatever asynchronous method you prefer.

        Args:
            key (str): key for the witness IP information that is being put into the DHT.
            payload (bytes): latest witness IP information.

        Returns:
            str: the result key that the result will be available under in :attr:`results`.
        """
        return self._generic_request_sync(IpPut, key, payload)


@contextmanager
def get_client(name, server):
    """Constructs a TCP client that can interact with the TCP server at 
    port `server_port`.

    Args:
        name (str): name to assign to the client for logging.
        server (str): of the format `host:port` to connect to.
    """
    host, port = server.split(':')
    port = int(port)
    log.debug(f"Creating doer-compatible TCP client `{name}` for {server}.")
    with clienting.openClient(clienting.Client, host=host, port=port, reconnectable=True, timeout=10) as client:
        result = DhtTcpClient(name, client, tock=TOCK)
        yield result

    result.shutdown()


class DhtClient(object):
    """A high-level client for performing indirect key-event state and witness
    IP information lookups.

    Attributes:
        auto_created (bool): when True, the TCP client was auto-created and
            must be cleaned up on shutdown.
    """
    def __init__(self, tcp_client: DhtTcpClient = None, host="localhost", port=23700):
        self.auto_created = False
        if tcp_client is None:
            self.auto_created = True
            client = clienting.Client(host=host, port=port, reconnectable=True, timeout=10)
            client.reopen()
            tcp_client = DhtTcpClient("DhtClient", client)
            
        self.client = tcp_client


    def close(self):
        """Cleans up the TCP client connection.
        """
        self.client.client.close()        


    async def get_kes_witnesses(self, kes: Serder):
        """Retrieve IP information from the DHT for a list
        of witnesses in a Key Event State (KES) message.

        Args:
            kes (Serder): most recent key-event state.

        Returns:
            list: of witness IP information dictionaries.
        """
        from .kes import get_witnesses_from_kes
        witnesses = await asyncio.gather(*[self.client.get_ip(w) 
                                           for w in get_witnesses_from_kes(kes)])
        return witnesses


    async def get_witness_ip(self, w: str):
        """Gets the witness IP information for a witness `w`.

        Args:
            w (str): public key of the witness to retrieve IP information for.
        """
        from .ipl import parse_verify_witness_ip
        payload = await self.client.get_ip(w)
        return parse_verify_witness_ip(payload)


    async def set_witness_ip(self, signer:Signer, ip4:str=None, ip6:str=None):
        """Updates the IP address for the *non-transferable* witness identifier
        represented by `signer`. Note that the public key registered with the
        DHT will be obtained from :attr:`Signer.verfer`.

        Args:
            signer (Signer): used to sign the request passed to the DHT.
            ip4 (str): IPv4 address that this witness is listening at.
            ip6 (str): IPv6 address that this witness is listening at.
        """
        from .ipl import build_witness_ip
        payload = build_witness_ip(signer, ip4, ip6)
        r = await self.client.put_ip(signer.verfer.qb64, payload)

        return r


    async def get_event_log(self, kes: Serder, w: Verfer):
        """Retrieves the full event log for `kes` from witness `w`.

        Args:
            kes (Serder): latest key event state to get full event log for.
            w (Verfer): representing the witness holding the key event log.
        """
        return {}