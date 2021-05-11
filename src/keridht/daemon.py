"""Background Server Daemon for keridht.
"""
from os import path
import argparse
from functools import partial
import signal, threading
import logging
import logging.config

from keridht.utility import relpath
from keridht import msg
from keridht.base import exhandler, bparser
from keridht.app import (start, shutdown as shutdown_app, DEFAULT_NODE_PORT, 
                         DEFAULT_API_PORT, DEFAULT_HOST, set_throttle)

logfile = relpath("../logging.conf")

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.config.fileConfig(logfile, disable_existing_loggers=False)
log = logging.getLogger(__name__)


DHT_BOOTSTRAP = "keri.digitalnation.io:23713"
"""str: hostname and port of the Digital Nation bootstrap node for the KERI DHT.
"""
shutdown = threading.Event()
"""threading.Event: for signaling shutdown has been triggered for the script.
"""


def examples():
    """Prints examples of using the script to the console using colored output.
    """
    script = "Kademlia KERI Discovery DHT"
    explain = ("This script starts a node that can participate in KERI DHT.")
    contents = [(("Run the DHT node using default configuration info."),
                 "keridhtd",
                 "A copy of the default configuration is included in package data for keridht."),
                 (("Run the DHT node using default configuration info, but slow execution speed."),
                 "keridhtd --throttle 0.1",
                 "The throttle parameter sets how many seconds to wait between iterations of "
                 "the coroutine scheduler."),
                 (("Run the DHT node using localhost"),
                 "keridhtd --local",
                 "This assumes a localhost bootstrap node."),
                 (("Debug a DHT node by logging all DHT requests (i.e., not TCP server, but DHT node)"),
                 "keridhtd --dhtlog ~/temp/dht.log",
                 "Normal logs for the TCP server and dependencies and put to stdout "
                 "using the configuration in logging.conf"),
                 (("Run a completely customized DHT node and TCP server."),
                 "keridhtd --host keri.mydomain.com --port 123456 --boostrap keri.bootstrap.com",
                 "While you can technically also change the DHT node port (using --dhtport), we "
                 "recommend using the default port.")]
    required = ("Requires OpenDHT dependencies are correctly configured if pip couldn't auto-install them.")
    output = ("")
    details = ("")
    outputfmt = ("")

    msg.example(script, explain, contents, required, output, outputfmt, details)


script_options = {    
    "--host": {"help": "Specify the host name to bind the API to.", "default": DEFAULT_HOST},
    "--port": {"help": "Specify the port to bind the API to.", "type": int, "default": DEFAULT_API_PORT},
    "--dhtport": {"help": "Port to bind the DHT node to.", "type": int, "default": DEFAULT_NODE_PORT},
    "--bootstrap": {"help": "Specify the hostname and port of a bootstrap node to connect to. "
                            "If not specified, default bootstrap nodes from ClearFoundation are used.",
                    "default": "default"},
    "--local": {"help": "When specified, run using the localhost configuration for testing.",
                "action": "store_true"},
    "--dhtlog": {"help": "Path to a file to store DHT node logs in; if not specified, "
                         "the DHT server will not log anything."},
    "--throttle": {"help": "Number of seconds to wait between buffer checks. This "
                           "is a global threshold parameter to control execution speed. "
                           "Larger numbers make the node run slower.", "default": 0.01}
}
"""dict: default command-line arguments and their
    :meth:`argparse.ArgumentParser.add_argument` keyword arguments.
"""


def _parser_options():
    """Parses the options and arguments from the command line."""
    pdescr = "KERI Indirect Discovery DHT"
    parser = argparse.ArgumentParser(parents=[bparser], description=pdescr)
    for arg, options in script_options.items():
        parser.add_argument(arg, **options)

    args = exhandler(examples, parser)
    if args is None:
        return

    return args


def get_local_script_args(index=0, logdir=None):
    """Gets the script arguments as a list that can be passed to subprocess
    for creating a local DHT node and API server.

    Args:
        index (int): index to use for port incrementing from the default values.
        logdir (str): path to the folder to store log files in.

    Returns:
        dict: keys are `args` which has the argument list that can be passed to
            :class:`subprocess.Popen`, `port` the incremented API server port, and
            `dhtport` the incremented port number of the DHT node.
    """
    tweakers = ["--port", "--dhtport"]
    args = ["keridhtd"]
    result = {}
    for key in tweakers:
        args.append(key)
        incremented = script_options[key]["default"] + index
        args.append(str(incremented))
        result[key[2:]] = incremented

    # This is explicitly a locally-run node and API server.
    args.append("--local")
    if index == 0:
        args.extend(["--bootstrap", "first"])
    if logdir is not None:
        args.extend(["--dhtlog", path.join(logdir, f"dht-{index}")])

    result["args"] = args
    return result


def stop(signal, frame):
    """Cleans up the nodes, event loops, asyncio, etc.
    """
    global shutdown
    msg.okay(f"SIGNAL {signal}: Cleaning up DHT node and asyncio for {frame}")
    shutdown.set()
    shutdown_app()


def configure_shutdown():
    """Configures shutdown of the application from the main thread.
    """
    thread_name = threading.current_thread().name
    log.debug(f"Configure shutdown called from {thread_name}.")
    if thread_name == "MainThread":
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        log.debug(f"SIGINT and SIGTERM wired to shutdown using {stop}.")


def run(**args):
    """Starts the local KERI DHT node and API server.
    """
    # Get the default bootstrap configuration for the existing DHT network.
    if args["bootstrap"] == "default":
        if args["local"]:
            args["bootstrap"] = "localhost:23713"
        else:
            args["bootstrap"] = DHT_BOOTSTRAP
    elif args["bootstrap"] == "first":
        # This is the first/bootstrap node in the network. That corresponds
        # to `None`.
        args["bootstrap"] = None

    set_throttle(args["throttle"])

    start(clients=None, **args)


def main(args=None):
    args = _parser_options() if args is None else args
    configure_shutdown()
    run(**args)


if __name__ == '__main__':  # pragma: no cover
    main()