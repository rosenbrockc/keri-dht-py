import pytest
import os
import logging
import time

from keridht.app import (DEFAULT_HOST, DEFAULT_NODE_PORT, DEFAULT_API_PORT,
                            start, shutdown, start_local)
from keridht.client import get_client
from keridht.testing import TestClient


log = logging.getLogger(__name__)

N_SERVERS = 2
"""int: number of servers and DHT nodes to start locally for the unit test.
"""
N_CLIENTS = 1
"""int: number of clients to target to *each* TCP server for generating
DHT API requests.
"""

@pytest.fixture(scope="session")
def N0():
    return start_local(0)


@pytest.fixture(scope="session")
def nodes(N0):
    result = {0: N0}
    for i in range(1, N_SERVERS-1):
        log.debug(f"N{i}: using {N0['node']} for bootstrapping.")
        result[i] = start_local(i)

    return result


@pytest.fixture(scope="session")
def clients(nodes):
    """Returns a set of clients (one for each node/API server) so that we
    can simulate a DHT network locally.
    """
    log.warning("Waiting 5 seconds for DHT node network to come up...")
    time.sleep(5)

    result, contexts = [], []
    try:
        for i, n in nodes.items():
            log.info(f"Setting up {N_CLIENTS} API clients targeting {n['server']}.")
            for c in range(N_CLIENTS):
                client_context = get_client(f"test-client-{i}:{c}", n["server"])
                result.append(client_context.__enter__())
                contexts.append(client_context)

        # Finally, handle the clients for the node that runs in the pytest process.
        final_server = f"{DEFAULT_HOST}:{DEFAULT_API_PORT+N_SERVERS-1}"
        for c in range(N_CLIENTS):
            client_context = get_client(f"test-client-{N_SERVERS-1}:{c}", final_server)
            result.append(client_context.__enter__())
            contexts.append(client_context)
        
        yield result
    finally:
        for ctx in contexts:
            ctx.__exit__(None, None, None)


@pytest.fixture(scope="session")
def client_tester(clients) -> TestClient:
    """Builds a client tester so that get/put tests can be simulated on the
    same scheduler.
    """
    return TestClient("client-tester", clients, maxtests=5, tock=0.5)


@pytest.fixture(scope="session")
def server(clients, client_tester, N0):
    """Returns the first non-bootstrap node in the DHT.
    """
    port, dhtport = N0["port"], N0["dhtport"]
    index = N_SERVERS-1
    yield start(clients + [client_tester], port=port+index, dhtport=dhtport+index, 
                bootstrap=N0["node"])

    log.info("Pytest server fixture is cleaning up now.")
    shutdown()