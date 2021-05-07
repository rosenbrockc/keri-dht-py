"""Module for configuring and verifying witness IP addresses.
"""
import msgpack
import socket
import logging

from keri.core.coring import Signer, Verfer, MtrDex
from keri.help.helping import nowIso8601, fromIso8601

log = logging.getLogger(__name__)


BPACK = None

def parse_verify_witness_ip(payload: bytearray):
    """Verifies the signature on the witness IP address payload, returns
    it as a python object if valid.

    .. note:: This method will *delete* the correct number of bytes
        required to verify the next witness IP information payload from
        `payload` so that this method can be used in a streaming context.

    Args:
        payload (bytes): result of :func:`build_witness_ip`.

    Returns:
        dict: with keys ['ip4', 'ip6', 'pk', 'timestamp'] containing
            the IPv4, IPv6 addresses for the witness with public key
            `pk`; `timestamp` is the last time that the IP address
            was set.
        None: if the verification failed or there was another unhandled
            exception.
    """
    try:
        return _verify_witness_ip(payload)
    except:
        log.error("While parsing and verifying witness IP information", 
                  exc_info=log.getEffectiveLevel()==logging.debug)


def _verify_witness_ip(payload: bytes) -> dict:
    """Verifies the signature on the witness IP address payload, returns
    it as a python object if valid.

    .. note:: This method will *delete* the correct number of bytes
        required to verify the next witness IP information payload from
        `payload` so that this method can be used in a streaming context.

    Args:
        payload (bytes): result of :func:`build_witness_ip`.

    Returns:
        dict: with keys ['ip4', 'ip6', 'pk', 'timestamp'] containing
            the IPv4, IPv6 addresses for the witness with public key
            `pk`; `timestamp` is the last time that the IP address
            was set.
        None: if the verification failed or there was another unhandled
            exception.
    """
    bsize, ssize = int(payload[0:3].decode(), 16), int(payload[3:6].decode(), 16)
    body_end = 6+bsize
    b = payload[6:body_end]

    body = msgpack.unpackb(b)
    signature = payload[body_end:body_end+ssize]

    # Note, we use the code MtrDex.Ed25519N by default since the witness
    # key states are not transferrable. Since we are using fully-qualified
    # crypto material, we don't have to worry about it anyway.
    verfer = Verfer(qb64b=body["v"])
    result = None
    if not verfer.verify(signature, b):
        result = {
            "ip4": socket.inet_ntop(socket.AF_INET, body["4"]),
            "ip6": socket.inet_ntop(socket.AF_INET6, body["6"]),
            "verfer": verfer,
            "timestamp": fromIso8601(body["t"])
        }
    else:
        log.info(f"Verification of witness IP information signature failed: {body['v']}.")

    del payload[0:body_end+ssize]
    return result


def build_witness_ip(signer:Signer, ip4:str=None, ip6:str=None):
    """Updates the IP address for the *non-transferable* witness identifier
    represented by `signer`. Note that the public key registered with the
    DHT will be obtained from :attr:`Signer.verfer`.

    Args:
        signer (Signer): used to sign the request passed to the DHT.
        ip4 (str): IPv4 address that this witness is listening at.
        ip6 (str): IPv6 address that this witness is listening at.

    Returns:
        bytearray: payload that can be sent directly to the DHT using a
            :class:`DhtTcpClient`.
    """
    assert ip4 is not None or ip6 is not None
    body = {
        "v": signer.verfer.qb64b,
        "4": None if ip4 is None else socket.inet_pton(socket.AF_INET, ip4), # 4 bytes
        "6": None if ip6 is None else socket.inet_pton(socket.AF_INET6, ip6), # 16 bytes
        "t": nowIso8601() # 32 bytes
    }
    # For ED25519 verfer, the total msgpack body will be ~128 bytes. To make room
    # for additional (and perhaps quantum-proof) public keys later, we'll allow
    # a maximum of 1KB for the Witness IP payload. So we allow a maximum of three
    # bytes as the leading prefix which specifies how large the body payload is.

    # Along the same lines, let's leave 3 bytes to specify the signature size
    # Later we will have to support possibly multiple signature types, not
    # just ED25519.
    b = msgpack.packb(body)
    s = signer.sign(b).raw
    bsize, ssize = f"{len(b):03x}".encode(), f"{len(s):03x}".encode()

    return bytearray(bsize + ssize + b + s)