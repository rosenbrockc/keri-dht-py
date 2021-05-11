import logging
import logging.config
from os import environ, path
from hio.base.doing import Doist
import atexit
import signal

from keridht.node import DhtDoer, dhtserver
from keridht.utility import relpath, execute


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.config.fileConfig(relpath('../logging.conf'), disable_existing_loggers=False)
log = logging.getLogger(__name__)


DEFAULT_HOST = "localhost"
"""str: default host to bind to for the API server if not specified.
"""
DEFAULT_API_PORT = 23700
"""int: default port number to bind the API server to if unspecified.
"""
DEFAULT_NODE_PORT = 23713
"""int: default port number to bind the DHT node to if unspecified.
"""


HOST = environ.get("API_HOST", DEFAULT_HOST)
"""str: name of the API server host name to bind to.
"""
PORT = int(environ.get("API_PORT", DEFAULT_API_PORT))
"""int: port number that the API should bind to and server from.
"""
NODE_PORT = int(environ.get("NODE_PORT", DEFAULT_NODE_PORT))
"""int: port on which to run the DHT node.
"""
TOCK = 0.1
"""float: how often to run the scheduler for hio.
"""

DHT_NODE = None
"""DhtDoer: that has getter and putter processors running on hio.
"""
DOIST = None
"""Doist: scheduler for executing the hio doers.
"""
APP_CONTEXT = None
"""generator: preserving the local context needed to clean-up the TCP server.
"""
APP = None
"""serving.Server: server that will process TCP requests for DHT info.
"""
PROCESSES = []
"""list: of :class:`subprocess.Popen` for any subprocess node/server combinations
started with :func:`start_local`.
"""
FILES = []
"""list: of `tuple` with handles to `stdout` and `stderr` files for each of the
processes in :data:`PROCESSES`.
"""


def set_throttle(throttle):
    """Changes the throttling parameter for the hio doist.

    Args:
        throttle (float): number of seconds between doist executions.
    """
    global TOCK
    TOCK = throttle
    if DOIST is not None:
        DOIST.tock = throttle


def start_local(index=0, virtualenv="keri", logdir=None):
    """Starts a local subprocess that includes an additional DHT node on an
    incremented port (based on index) as well as an API server.

    Args:
        index (int): increment from the default port numbers to use for the
            new node and API server.
        virtualenv (str): name of a python virtualenvironment to activate and
            use for the execution. Set to `None` if you want to run in the 
            default shell. This assumes that `virtualenv` and `virtualenvwrapper`
            are both installed on the local machine. See :func:`keridht.utility.execute`
            for more details.
        logdir (str or Path): path to a folder to store the logfiles in for `stdout`
            and `stderr`.

    Returns:
        dict: with the process, host and port configurations for the node and API server.
            Keys are `(process, host, port, dhtport, server, node)`, where server is 
            the `host:port` of the API server; and `node` is the `host:port` of the DHT
            node.
    """
    global PROCESSES, FILES
    from keridht.daemon import get_local_script_args

    if logdir is None:
        logdir = path.expanduser("~/temp")
    script_args = get_local_script_args(index, logdir)

    out = open(path.join(logdir, f"stdout-{index}"), 'w')
    err =open(path.join(logdir, f"stderr-{index}"), 'w')
    FILES.append((out, err))
     
    result = execute(script_args["args"], wait=False, venv=virtualenv, stdout=out, stderr=err)

    # Since we aren't waiting for it to finish executing (it is a process that runs
    # forever), we append the process to our global process list so it can be cleaned
    # up nicely on shutdown.
    PROCESSES.append(result["process"])

    return {
        "process": result["process"],
        "host": "localhost",
        "port": script_args["port"],
        "dhtport": script_args["dhtport"],
        "server": f"localhost:{script_args['port']}",
        "node": f"localhost:{script_args['dhtport']}"
    }


def create(host=None, port=None, dhtport=None, bootstrap=None, dhtlog=None):
    """Creates the TCP server `Doer` that interfaces the DHT for requests.

    Args:
        host (str): host name on which to bind the TCP server. Note that if not
            specified, the value is looked up from the `API_HOST` environment
            variable. If that variable is missing, `localhost` is used.
        port (int): port number to bind the socket to. If not specified, then the
            value is looked up from the `API_PORT` environment variable. If that 
            variable is not set, then the default value of `23700` is used.
        dhtport (int): port to bind the DHT node to. Looked up from `NODE_PORT`
            environment variable; otherwise default is 23713.
        bootstrap (str): `host:port` of the DHT node to bootstrap this one from.
        dhtlog (str): path to a file to use for storing the logs from the DHT node.
            If `None`, then DHT node will *not* log anything.
    """
    global DHT_NODE, DOIST

    # Set the default port and host names.
    if dhtport is None:
        dhtport = NODE_PORT
    if host is None:
        host = HOST
    if port is None:
        port = PORT

    DHT_NODE = DhtDoer(dhtport, TOCK, bootstrap_node=bootstrap, is_bootstrap=(bootstrap is None),
                       logfile=dhtlog)
    DOIST = Doist(real=True, doers=[DHT_NODE])

    if host == "localhost":
        log.warning("The API host is only binding to localhost; won't be publicly accessible.")

    with dhtserver(DHT_NODE, host, port) as server:
        yield server


def start(clients=None, host=None, port=None, dhtport=None, bootstrap=None, 
          dhtlog=None, **kwargs):
    """Creates the application context, TCP server instance, DHT nodes and 
    DHT doers that support its methods.

    Args:
        clients (iterable): of `Doers` to add to the doist.
        host (str): host name on which to bind the TCP server. Note that if not
            specified, the value is looked up from the `API_HOST` environment
            variable. If that variable is missing, `localhost` is used.
        port (int): port number to bind the socket to. If not specified, then the
            value is looked up from the `API_PORT` environment variable. If that 
            variable is not set, then the default value of `23700` is used.
        dhtport (int): port to bind the DHT node to. Looked up from `NODE_PORT`
            environment variable; otherwise default is 23713.
        bootstrap (str): `host:port` of the DHT node to bootstrap this one from.
        dhtlog (str): path to a file to use for storing the logs from the DHT node.
            If `None`, then DHT node will *not* log anything.
        kwargs (dict): additional keyword arguments that we don't use; allows the `**`
            notation.
    """
    log.debug(f"Ignoring superfluous keyword arguments {kwargs}.")

    global APP, APP_CONTEXT, DOIST
    APP_CONTEXT = create(host=host, port=port, dhtport=dhtport, bootstrap=bootstrap,
                         dhtlog=dhtlog)
    APP = next(APP_CONTEXT)
    
    # Start the loop that processes the hio generators.
    DOIST.doers.append(APP)
    if clients is not None:
        DOIST.doers.extend(clients)

    DOIST.do()


def shutdown():
    """Shuts down the DHT doers and nodes cleanly.
    """
    if DHT_NODE is not None and not DHT_NODE.shutting_down:
        log.info("Shutting down DHT node and processors from main app.")
        DHT_NODE.shutdown()

    if APP_CONTEXT is not None:
        try:
            next(APP_CONTEXT)
        except StopIteration:
            log.debug("App context iterator for cleanup has already been called.")

    # Shut down any of the subprocess nodes and servers.
    for sp in PROCESSES:
        sp.send_signal(signal.SIGINT)

    # Also clean up our files for stdout and stderr redirection.
    for out, err in FILES:
        if out is not None:
            out.close()
        if err is not None:
            err.close()


atexit.register(shutdown)