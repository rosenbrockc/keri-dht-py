[![Docker Image](https://github.com/rosenbrockc/keri-dht-py/actions/workflows/docker-image.yml/badge.svg)](https://github.com/rosenbrockc/keri-dht-py/actions/workflows/docker-image.yml)

# KERI Discovery using a DHT in Python
## keri-dht-py

Kademlia based DHT mechanism for KERI Discovery written in Python but leveraging C libraries for Kademlia. For more details, please visit [KERI Resources](https://keri.one/keri-resources/), in
particular, the [whitepaper](https://github.com/ryanwwest/papers/blob/master/whitepapers/keridemlia.pdf). The [specifications for KERI](https://github.com/decentralized-identity/keri)
are also a good place to go for more information.

## Quickest Start

`keridht` is a python package and can be installed directly from PyPI.

```
pip install keridht
daemon.py --examples
```

The second command will print a set of examples of how to start the local DHT node,
and which options are available for configuration. Note that on some systems, the
python `opendht` package does not build automatically using `cython`. In these cases,
you will either need to use the docker container, or build manually as described
below.

## Docker Container

We plan to release a docker container with the DHT soon.

# Background and Dependencies

`keridht` integrates the [core KERI libraries](https://github.com/SmithSamuelM/keripy)
(https://github.com/decentralized-identity/keri) with [OpenDHT](https://github.com/savoirfairelinux/opendht)
using python. To be as performant as possible, the I/O is handled asynchronously
using [hio](https://github.com/ioflo/hio).

## Manual Build Process

To manually build `opendht` on MacOS. Make sure you are in the python `virtualenv`
you intend to use before running this install script.

```
./install_deps.sh
```

# Running Unit Tests

`keridht` uses `pytest` to run all the unit tests. To run all the pre-configured
tests, just run `pytest` from this repository root.