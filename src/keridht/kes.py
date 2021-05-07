"""Module exposing functions for verifying Key Event States (KES)
"""
import logging
import asyncio
import msgpack
from collections import deque
from hio.base.doing import Doer

from keri.core.eventing import Kever, Kevery, Serder

from .client import DhtClient, DhtTcpClient

log = logging.getLogger(__name__)

class KeyEventStateInvalidError(Exception):
    """Raised when a key event state could not be verified using the
    witnesses event log information.
    """


def get_witnesses_from_kes(kes: Serder):
    """Retrieves the current list of witnesses for a KES.

    Returns:
        set: of witness identifiers.
    """
    result = set(kes.ked["w"])
    wa = set(kes.ked["ee"].get("wa", []))
    wr = set(kes.ked["ee"].get("wr", []))

    # Some extra sanity checks to make sure this is a totally valid
    # witness change event.
    assert len(wa & wr) == 0
    assert len(wa & result) == 0
    assert result | wr == result
    
    if "wa" in kes.ked["ee"]:
        result = result | set(kes.ked["ee"]["wa"])
    if "wr" in kes.ked["ee"]:
        result = result - set(kes.ked["ee"]["wr"])

    return result


def verify_event_logs(logs: list):
    """Verifies that all the specified logs have consensus on the 
    current key event state.
    """
    pass


async def verify_kes(kes: Serder, client: DhtClient):
    """Verifies the cryptographic material the KES against the witnesses
    and their event logs.
    """
    witnesses = await client.get_kes_witnesses(kes)
    logs = await asyncio.gather(*[client.get_event_log(kes, w["verfer"]) for w in witnesses])
    return verify_event_logs(logs)


async def parse_verify_kes(key: str, payload: bytearray, client: DhtClient) -> list:
    """Parses the payload to retrieve the key event state and then
    replays the event log for accuracy and verification. This includes
    grabbing event log copies from witnesses.

    Args:
        key (str): the qb64 of the kever to process.

    Returns:
        Kever: that have been verified by replaying witness event logs.
    """
    bsize = int(payload[0:4].decode(), 16)
    raw = payload[4:4+bsize]
    kevery = Kevery(raw)
    kevery.process()
    kever = kevery.kever
    if not await verify_kes(kever.state(), client):
        raise KeyEventStateInvalidError(f"While verifying {kever}.")
        
    return kever


async def build_kes(kes: Serder, client: DhtClient):
    """Builds a key-event state message payload to send to the DHT.

    .. note:: This method first validates that the KES being sent has
        valid signatures and that the KES is valid with all the witnesses.
        This involves making requests for the event log to all witnesses.

    Raises:
        KeyEventStateInvalidError: if the KES could not be verified.
    """
    if not await verify_kes(kes, client):
        raise KeyEventStateInvalidError(f"Could not verify witness event logs.")

    # The KES is valid, we can push it to the DHT.
    bsize = f"{len(kes.raw):04x}".encode()
    return bytearray(bsize + kes.raw)