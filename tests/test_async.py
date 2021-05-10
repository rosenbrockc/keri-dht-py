import pytest
import logging

log = logging.getLogger(__name__)


def test_async(client_tester, server):
    """Allows all the text fixtures to run through all of their tests and interactions.
    """
    log.info(f"Running tests using fixture {client_tester} against {server}.")
    print(client_tester.timing)