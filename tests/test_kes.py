import pytest

from keri.core.coring import MtrDex, Nexter, Salter, Serder, Ilks, Prefixer
from keri.core.eventing import Serials, Versify, Kever

from keridht.kes import build_kes, parse_verify_kes
from keridht.client import DhtClient

@pytest.fixture
def kever():
    # Setup inception key event dict
    salt = b'\x05\xaa\x8f-S\x9a\xe9\xfaU\x9c\x02\x9c\x9b\x08Hu'
    salter = Salter(raw=salt)
    # create current key
    sith = 1  #  one signer
    #  original signing keypair transferable default
    skp0 = salter.signer(path="A", temp=True)
    keys = [skp0.verfer.qb64]

    # create next key
    #  next signing keypair transferable is default
    skp1 = salter.signer(path="N", temp=True)
    nxtkeys = [skp1.verfer.qb64]
    # compute nxt digest
    nexter = Nexter(keys=nxtkeys)
    nxt = nexter.qb64
    assert nxt == "E_d8cX6vuQwmD5P62_b663OeaVCLbiBFsirRHJsHn9co"  # transferable so nxt is not empty

    sn = 0  #  inception event so 0
    toad = 0  # no witnesses
    nsigs = 1  #  one attached signature unspecified index

    ked0 = dict(v=Versify(kind=Serials.json, size=0),
                i="",  # qual base 64 prefix
                s="{:x}".format(sn),  # hex string no leading zeros lowercase
                t=Ilks.icp,
                kt="{:x}".format(sith), # hex string no leading zeros lowercase
                k=keys,  # list of signing keys each qual Base64
                n=nxt,  # hash qual Base64
                wt="{:x}".format(toad),  # hex string no leading zeros lowercase
                w=[],  # list of qual Base64 may be empty
                c=[],  # list of config ordered mappings may be empty
                )

    # Derive AID from ked
    aid0 = Prefixer(ked=ked0, code=MtrDex.Ed25519)
    assert aid0.code == MtrDex.Ed25519
    assert aid0.qb64 == skp0.verfer.qb64 == 'DBQOqSaf6GqVAoPxb4UARrklS8kLYj3JqsR6b4AASDd4'

    # update ked with pre
    ked0["i"] = aid0.qb64

    # Serialize ked0
    tser0 = Serder(ked=ked0)

    # sign serialization
    tsig0 = skp0.sign(tser0.raw, index=0)

    # verify signature
    assert skp0.verfer.verify(tsig0.raw, tser0.raw)

    return Kever(serder=tser0, sigers=[tsig0])


@pytest.fixture
def client() -> DhtClient:
    """Constructs a DHT client for put/get operations to the DHT.
    """
    return DhtClient()


@pytest.mark.asyncio
async def test_kes(kever: Kever, client: DhtClient):
    """Tests the putting and getting of key-event state in the DHT.
    """
    payload = await build_kes(kever.state(), client)
    kes = kever.state()
    verfers = kes.verfers
    vqbs = ','.join(v.qb64 for v in verfers)
    kevers = await parse_verify_kes(vqbs, payload, client)
    print(kevers)