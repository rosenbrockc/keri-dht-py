import abc
import logging
from collections import namedtuple
from enum import auto
import msgpack

from keridht.node import LookupValues, PREFIX_SIZE, Requester
from .utility import AutoName


GetRequest = namedtuple("GetRequest", ["callback", "identifier", "key"])
PutRequest = namedtuple("PutRequest", ["callback", "key", "payload"])

log = logging.getLogger(__name__)


class METHODS(AutoName):
    GET = auto()
    PUT = auto()

class MethodResource(abc.ABC):
    """Abstract method resource handler for the TCP server.

    Args:
        dhtdoer (DhtDoer): doer whose queues handle processing events against
            the network of DHT nodes.
        requester (Requester): representing the client endpoint that made the request.
            It holds onto results until they can be sent back to the client.
    """
    def __init__(self, dhtdoer, requester):
        self.dhtdoer = dhtdoer
        self.requester = requester

        if requester is None or not isinstance(requester, Requester):
            raise ValueError("No client requester specified to return values to.")


    @abc.abstractmethod
    def parse(self, raw, size, version):
        """Parses the data for the request from `raw` and sets internal
        state for this resource object.

        Args:
            raw (bytearray): raw bytes of the request.
            size (int): total size of the payload from parsing the request header.
            version (Versionage): version number of the request to handle.
        """


    @abc.abstractmethod
    def callback(self, *args):
        """Callback creator that registers callback functions with the DHT doer
        so that results can be passed back to the original requester.

        Args:
            args (tuple): positional arguments specific to the method endpoint.
        """


    @abc.abstractmethod
    def execute(self):
        """Executes the method backend requests against the DHT.
        """

    @classmethod
    def unmarshal(cls, result, verbose=True):
        """Unmarshals a result from bytes into a `dict`.

        Args:
            result (bytearray): taken from the TCP buffer.
            verbose (bool): when True, expand the names of the `dict` keys to be more
                verbose.
        """
        log.debug(f"Unmarshaling {bytes(result)} ({len(result)}) received by client.")
        U = msgpack.unpackb(result)
        if verbose:
            U = default_result_verbose(U)

        return U


    @abc.abstractclassmethod
    def marshal(cls, result):
        """Marshals the result added to a requester's results (a `dict`) into the
        bytearray format needed for sending over the wire.
        """


    @abc.abstractclassmethod
    def build_request(cls, *args):
        """Builds the binary array that can be submitted as a request to this server
        resource from a TCP client.

        Args:
            args (tuple): of arguments specific to the resource type.
        """


    @abc.abstractproperty
    def fields(self):
        """Returns a list of all keys available in the result fields of this method.
        """


def default_result_verbose(d):
    """Converts the terse default result `dict` returned by :func:`prepare_default_result_dict`
    into a more verbose version.
    """
    v = {
        "key": d["k"],
        "done": d["d"],
        "nodes": [{
            "id": n["i"].decode("utf8"),
            "address": n["a"].decode("utf8"),
            "expired": n["x"]
        } for n in d["n"]]
    }
    if "r" in d:
        v["result"] = d["r"]

    return v


def prepare_default_result_dict(key, done, nodes):
    """Prepares the default result `dict` using common values returned by any
    operation on the DHT.

    Returns:
        dict: with keys `(k, d, n)` for the key, done and nodes; `n` is a list
            of `dict` with keys `(i, a, x)` for id, address, and expiration.
    """
    d = {
        "k": key,
        "d": done,
    }
    nb = []
    for n in nodes:
        _node = n.getNode()
        nb.append({
            "i": n.getId().toString(),
            "a": _node.getAddr(),
            "x": _node.isExpired()
        })
    d["n"] = nb

    return d


class DhtGetterResource(MethodResource):
    """Implements a method resource that gets values from the DHT.
    """
    def __init__(self, dhtdoer, requester):
        super(DhtGetterResource, self).__init__(dhtdoer, requester)


    def parse(self, raw, size, version):
        """Extracts the request key to GET from the raw bytes.
        """
        # We extract the key from the `size` bytes following the initial header
        # which is strictly 12 bytes. Currently, the versions don't actually
        # influence the outcome of this parsing; but it could in the future.
        key = raw[PREFIX_SIZE:PREFIX_SIZE+size].decode("utf8")
        self.data = {
            "key": f"{self.endpoint}{key}"
        }


    def marshal(self, results: LookupValues, done, nodes):
        """Conforms the request result from :meth:`callback` into bytes.

        Args:
            results (LookupValues): DHT lookup key and values that the result pertains to.
        """
        d = prepare_default_result_dict(results.key, done, nodes)
        d["r"] = [r.data for r in results]
        R = msgpack.packb(d)
        return Requester.build_prefix(self.endpoint, METHODS.GET.name, len(R)) + R


    def callback(self, results: LookupValues, done, nodes):
        """Returns a function that appends a set of results for a DHT lookup
        to the output queue.

        Args:
            results (LookupValues): DHT lookup key and values that the result pertains to.
            done (bool): indicating whether the DHT request completed.
            nodes (list): of :class:`opendht.Node` indicating nodes involved in the result.
        """
        self.requester.results.append({
            "label": f"{self.endpoint}/get",
            "result": self.marshal(results, done, nodes),
            "done": done,
            "nodes": nodes,
            # This two removes the endpoint prefix that we added to allow keys to be 
            # unique among endpoints.
            "key": results.key[2:]
        })


    def execute(self):
        """Processes a single GET against the ID endpoint to retrieve a KES.
        """
        key = self.data["key"]
        request = GetRequest(self.callback, id(self), key)
        log.debug(f"Adding {request.key[0:10]}... to DHT Getter Doer's deque.")
        self.dhtdoer.getter.queue.append(request)


    @property
    def fields(self):
        """Returns a list of all keys available in the result fields of this method.
        """
        return ["label", "result", "done", "nodes", "key"]


    @classmethod
    def build_request(cls, key, endpoint):
        """Builds the binary array that can be submitted as a request to this server
        resource from a TCP client.

        .. note:: The endpoint is prepended to the key so that if the key is a public
            key that it will have a distinct key based on the endpoint being requested.

        Args:
            key (str): string specifying the key to download.
        """
        bkey = key.encode("utf8")
        prefix = Requester.build_prefix(endpoint, METHODS.GET.name, len(bkey))
        return prefix + bkey


class DhtPutterResouce(MethodResource):
    """Implements a method resource that puts values from the DHT.
    """
    def __init__(self, dhtdoer, requester):
        super().__init__(dhtdoer, requester)

    def parse(self, raw, size, version):
        data = raw[PREFIX_SIZE:PREFIX_SIZE+size]
        d = msgpack.unpackb(data)
        self.data = {
            "key": f"{self.endpoint}{d['k']}",
            "payload": d["p"]
        }


    def marshal(self, result):
        """Conforms the request result from :meth:`callback` into bytes.
        """
        d = prepare_default_result_dict(result["key"], result["done"], result["nodes"])
        R = msgpack.packb(d)
        return Requester.build_prefix(self.endpoint, METHODS.PUT.name, len(R)) + R


    def callback(self, done, nodes):
        """Returns a function to use as the done callback for the put operation.
        """
        log.debug(f"Put completed for {self.data['key']}; done={done}; nodes={nodes}.")
        result = {
            "label": f"{self.endpoint}/put",
            "done": done,
            "nodes": nodes,
            # This two removes the endpoint prefix that we added to allow keys to be 
            # unique among endpoints.
            "key": self.data["key"][2:] 
        }
        result["result"] = self.marshal(result)
        self.requester.results.append(result)


    def execute(self):
        """Processes a single PUT request to update the KES for a given key.
        """        
        key, payload = self.data["key"], self.data["payload"]
        request = PutRequest(self.callback, key, payload)
        log.debug(f"Adding {request.key[0:10]}... to DHT Putter Doer's deque.")
        self.dhtdoer.putter.queue.append(request)


    @property
    def fields(self):
        """Returns a list of all keys available in the result fields of this method.
        """
        return ["label", "done", "nodes", "key"]


    @classmethod
    def build_request(cls, key, payload, endpoint):
        """Builds the binary array that can be submitted as a request to this server
        resource from a TCP client.

        .. note:: The endpoint is prepended to the key so that if the key is a public
            key that it will have a distinct key based on the endpoint being requested.

        Args:
            key (str or bytes): string specifying the key to put.
            payload (bytes): raw byte stream to put into the DHT.
        """
        d = {
            "k": key,
            "p": payload
        }
        bd = msgpack.packb(d)
        prefix = Requester.build_prefix(endpoint, METHODS.PUT.name, len(bd))
        return prefix + bd


class IdGet(DhtGetterResource):
    """Method resource for the `/id` endpoint's `GET` method.
    """
    endpoint = "id"

    def __init__(self, dhtdoer, requester):
        super().__init__(dhtdoer, requester)


    @classmethod
    def build_request(cls, key):
        """Builds the binary array that can be submitted as a request to this server
        resource from a TCP client.

        Args:
            key (str or bytes): byte string specifying the key to download.
        """
        return DhtGetterResource.build_request(key, IdGet.endpoint)


class IdPut(DhtPutterResouce):
    """Method resource for the `/id` endpoint's `PUT` method.
    """
    endpoint = "id"

    def __init__(self, dhtdoer, requester):
        super().__init__(dhtdoer, requester)


    @classmethod
    def build_request(cls, key, payload):
        """Builds the binary array that can be submitted as a request to this server
        resource from a TCP client.

        Args:
            key (str or bytes): byte string specifying the key to put.
            payload (bytes): raw byte stream to put into the DHT.
        """
        return DhtPutterResouce.build_request(key, payload, IdPut.endpoint)


class IpGet(DhtGetterResource):
    """Method resource for the `/ip` endpoint's `GET` method.
    """
    endpoint = "ip"

    def __init__(self, dhtdoer, requester):
        super().__init__(dhtdoer, requester)


    @classmethod
    def build_request(cls, key):
        """Builds the binary array that can be submitted as a request to this server
        resource from a TCP client.

        Args:
            key (str or bytes):  byte string specifying the key to download.
        """
        return DhtGetterResource.build_request(key, IpGet.endpoint)


class IpPut(DhtPutterResouce):
    """Method resource for the `/ip` endpoint's `PUT` method.
    """
    endpoint = "ip"

    def __init__(self, dhtdoer, requester):
        super().__init__(dhtdoer, requester)


    @classmethod
    def build_request(cls, key, payload):
        """Builds the binary array that can be submitted as a request to this server
        resource from a TCP client.

        Args:
            key (str or bytes): byte string specifying the key to put.
            payload (bytes): raw byte stream to put into the DHT.
        """
        return DhtPutterResouce.build_request(key, payload, IpPut.endpoint)