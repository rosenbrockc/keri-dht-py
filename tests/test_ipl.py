"""Tests the building and verification of witness IP information.
"""
from keri.core.coring import Signer, MtrDex
from keridht.ipl import build_witness_ip, parse_verify_witness_ip

import datetime

def test_ip_roundtrip():
    """Tests that a payload can be serialized to bytes and then deserialized
    with verification for a hard-coded secret.
    """
    s = Signer(qb64="AgjD4nRlycmM5cPcAkfOATAp8wVldRsnc9f1tiwctXlw",
               transferable=False)
    now = datetime.datetime.now(datetime.timezone.utc)
    payload = build_witness_ip(s, "10.0.0.8", "0a:ff:c2:43:91:5c::")
    r = parse_verify_witness_ip(payload)

    assert r is not None
    assert r["ip4"] == "10.0.0.8"
    assert r["ip6"] == "a:ff:c2:43:91:5c::"
    assert (r["timestamp"] - now).seconds < 5
    assert r["verfer"].raw == s.verfer.raw