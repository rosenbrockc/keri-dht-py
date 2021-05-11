#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

    You might be tempted to import things from __main__ later, but that will cause
    problems: the code will get executed twice:

    - When you run `python -m keridht` python will execute
      ``__main__.py`` as a script. That means there won't be any
      ``keridht.__main__`` in ``sys.modules``.
    - When you import __main__ it will get executed again (as a module) because
      there's no ``keridht.__main__`` in ``sys.modules``.

Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
from os import path
import argparse
import asyncio
import signal, threading
import logging
import logging.config
import json

from keri.core.coring import Signer

from keridht.client import DhtClient

from keridht.utility import relpath
from keridht import msg
from keridht.base import exhandler, bparser
from keridht.app import DEFAULT_API_PORT, DEFAULT_HOST

logfile = relpath("../logging.conf")

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.config.fileConfig(logfile, disable_existing_loggers=False)
log = logging.getLogger(__name__)


CLIENT = DhtClient()
"""DhtClient: for making requests to the TCP server.
"""
shutdown = threading.Event()
"""threading.Event: for signaling shutdown has been triggered for the script.
"""


def examples():
    """Prints examples of using the script to the console using colored output.
    """
    script = "KERI DHT Client"
    explain = ("This script is a CLI client for the DHT.")
    contents = [(("Get IP address information for a witness."),
                 "keridht get --witness --qb64 DBQOqSaf6GqVAoPxb4UARrklS8kLYj3JqsR6b4AASDd4",
                 "The argument is the qb64 of the witness Verfer."),
                 (("Update witness IP address information."),
                 "keridht put --witness --ip ip.json --qb64 AgjD4nRlycmM5cPcAkfOATAp8wVldRsnc9f1tiwctXlw",
                 "This time the argument is the qb64 secret to use for the signer. The "
                 "verfer is automatically extracted and updated. `ip.json` is a JSON "
                 "dictionary with IPv4 and IPv6 information. See examples."),
                 (("Get and verify the latest Key Event State for an identifier."),
                 "keridht get --kes --qb64 AgjD4nRlycmM5cPcAkfOATAp8wVldRsnc9f1tiwctXlw",
                 ""),
                 (("Post the latest Key Event State for an identifier to the DHT."),
                 "keridht put --kes --ked ked.json --qb64 AgjD4nRlycmM5cPcAkfOATAp8wVldRsnc9f1tiwctXlw",
                 "DHT servers will verify the state using full witness event logs. "
                 "--ked supplies a JSON (or other supported serialization format) "
                 "key event dictionary. The --qb64 in this case is the secret for the "
                 "Signer to make the update."),
                 (("Get IP address information for a witness from a specific TCP server."),
                 "keridht get --host my.favorite.host --port 123456 --witness --qb64 DBQOqSaf6GqVAoPxb4UARrklS8kLYj3JqsR6b4AASDd4",
                 "The host and port are of the TCP server, *not* the DHT node.")]
    required = ("")
    output = ("")
    details = ("")
    outputfmt = ("")

    msg.example(script, explain, contents, required, output, outputfmt, details)


script_options = {    
    "command": {"help": "Which action to perform against the DHT", 
                "choices": ["get", "put"]},
    "--host": {"help": "Specify the host name to bind the API to.", "default": DEFAULT_HOST},
    "--port": {"help": "Specify the port to bind the API to.", "type": int, "default": DEFAULT_API_PORT},    
    "--witness": {"help": "Apply the `command` to witness IP information.", 
                  "action": "store_true"},
    "--kes": {"help": "Apply the `command` to key event state information.",
                  "action": "store_true"},
    "--qb64": {"help": "Base64-encoded cryptographic matter. See examples."},
    "--ked": {"help": "For updating the key-event state, the key event dictionary."}
}
"""dict: default command-line arguments and their
    :meth:`argparse.ArgumentParser.add_argument` keyword arguments.
"""


def _parser_options():
    """Parses the options and arguments from the command line."""
    pdescr = "KERI DHT Client"
    parser = argparse.ArgumentParser(parents=[bparser], description=pdescr)
    for arg, options in script_options.items():
        parser.add_argument(arg, **options)

    args = exhandler(examples, parser)
    if args is None:
        return

    return args


def stop(signal, frame):
    """Cleans up the nodes, event loops, asyncio, etc.
    """
    global shutdown
    msg.okay(f"SIGNAL {signal}: Cleaning up DHT node and asyncio for {frame}")
    shutdown.set()

    if CLIENT is not None:
      CLIENT.close()


def configure_shutdown():
    """Configures shutdown of the application from the main thread.
    """
    thread_name = threading.current_thread().name
    log.debug(f"Configure shutdown called from {thread_name}.")
    if thread_name == "MainThread":
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        log.debug(f"SIGINT and SIGTERM wired to shutdown using {stop}.")


def print_save(result, args):
    """Prints/saves the result of the CLI command.
    """
    pass


async def _get_witness(args):
    """Runs the get witness IP information command.
    """
    log.debug(f"Getting witness IP information for {args['qb64']}.")
    r = await CLIENT.get_witness_ip(args["qb64"])
    print_save(r, args)
    return r


async def _put_witness(args):
    """Runs the put witness IP information command.
    """
    s = Signer(qb64=args["qb64"], transferable=False)
    target = path.abspath(path.expanduser(args["ip"]))
    if path.isfile(target):
        with open(target, 'r') as f:
            ip = json.load(f)
        log.debug(f"Witness IP information is being set to {ip}.")
    else:
        msg.warn(f"The IP information file {target} does not exist.")

    r = await CLIENT.set_witness_ip(s, **ip)
    print_save(r, args)
    return r


async def _get_kes(args):
    """Gets current key event state from the DHT.
    """
    raise NotImplementedError("CLI function `get kes` has not been implemented yet.")


async def _put_kes(args):
    """Updates the key event state in the DHT.
    """    
    raise NotImplementedError("CLI function `put kes` has not been implemented yet.")


CMD_PUT = "put"
"""str: command for putting data into the DHT.
"""
CMD_GET = "get"
"""str: command for getting info from the DHT.
"""
WITNESS_EXEC = {
    CMD_PUT: _put_witness,
    CMD_GET: _get_witness
}
"""dict: keys are one of the available commands; values
are functions for executing that command.
"""
KES_EXEC = {
  CMD_PUT: _put_kes,
  CMD_GET: _get_kes
}
"""dict: keys are one of the available commands; values
are functions for executing that command.
"""


def run(**args):
    """Starts the local KERI DHT node and API server.
    """
    CLIENT = DhtClient(host=args["host"], port=args["port"])
    target = None
    if args["witness"]:
        target = WITNESS_EXEC
    if args["kes"]:
        target = KES_EXEC

    x = None
    if target is not None:
        x = target.get(args["command"])

    if x is not None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(x(args))


def main(args=None):
    args = _parser_options() if args is None else args
    configure_shutdown()
    run(**args)


if __name__ == '__main__':  # pragma: no cover
    main()


