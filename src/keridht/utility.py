from os import path, chdir as os_chdir, getcwd, environ
from contextlib import contextmanager
from importlib import import_module
import logging, sys
from hashlib import sha256
from subprocess import Popen, PIPE
from enum import Enum
import base64

from . import msg

log = logging.getLogger(__name__)

reporoot = None
"""str: full path to the repository root for the code.
"""

def get_reporoot():
    """Gets the full path to the repository root.
    """
    import keridht
    global reporoot
    if reporoot is None:
        packpath = path.abspath(keridht.__file__)
        reporoot = path.dirname(path.dirname(packpath))

    return reporoot


get_reporoot()


@contextmanager
def chdir(target):
    """Context manager for executing some code within a different
    directory after which the current working directory will be set
    back to what it was before.

    Args:
        target (str): path to the directory to change into.
    """
    current = getcwd()
    try:
        os_chdir(target)
        yield target
    finally:
        os_chdir(current)


def relpath(s, reldir=None):
    """Returns the *repository-relative* path for the string `s`.

    Args:
        s (str): repository-relative path, see the examples.
        reldir (str): when specified, make the path relative to this folder
          instead of the *code* repository root.

    Examples:
        Suppose I have a repository at `/usr/opt/repo`, then:

        >>> relpath("./tests") == "/usr/opt/repo/tests"
        True
        >>> relpath("../other/docs") == "/usr/opt/other/docs"
        True
    """
    if reldir is None:
        reldir = reporoot
    with chdir(reldir):
        result = path.abspath(s)
    return result


def import_fqdn(fqdn):
    """Returns the object from the specified FQDN. Any exceptions raised will
    bubble up.
    Args:
        fqdn (str): '.'-separated list of `package.module.callable` to
          import. The callable will *not* be called.
    Returns:
        tuple: `(module, callable)`, where `module` is the module object that
        `callable` resides in.
    """
    # from importlib import import_module
    log.debug(f"Importing {fqdn} dynamically.")
    parts = fqdn.split('.')
    call = parts[-1]
    module = '.'.join(parts[0:-1])
    try:
        module = import_module(module)
    except ImportError:
        module = import_module(parts[0])
        if len(parts) > 2:
            for part in parts[1:-1]:
                module = getattr(module, part)
            
    call = getattr(module, call)
    return (module, call)


def execute(args, folder=None, wait=True, nlines=100, venv=None,
            printerr=True, env_vars=None, errignore=None, shell=True, **kwargs):
    """Executes the specified tuple that should include the command as
    first item and additional arguments afterward. See the
    documentation for :class:`subprocess.Popen` for details.
    Args:
        args (list): of `str`; first item should be command to
          execute; additional arguments following.
        folder (str): directory to switch into before executing the
          command.
        wait (bool): when True, block the current thread until
          execution completes; otherwise, returns immediately.
        nlines (int): by default, `stdout` and `stderr` are redirected to
          :data:`subprocess.PIPE`. This is the maximum number of lines that will
          be returned for large outputs (so that memory doesn't get overwhelmed
          by large outputs).
        venv (str): when not `None`, the name of a virtualenv to
          activate before running the command.
        printerr (bool): when True, if `stderr` is not empty, print
          the lines automatically.
        env_vars (dict): of environment variables to set before calling the
          execution. The variables will be set back after execution.
        errignore (str): if errors are produced that include this pattern, then
          they will *not* be printed to `stdout`.
        kwargs (dict): additional arguments that are passed directly
          to the :class:`subprocess.Popen` constructor.
    Returns:
        dict: with keys ['process', 'stdout', 'stderr'], where 'process' is the
        instance of the subprocess that was created; 'stdout' and 'stderr' are
        only included if they were set to :data:`subprocess.PIPE`.
    .. note:: If the output from 'stdout' and 'stderr' are too large, only the
      first 100 lines will be returned. Use parameter `nlines` to control output
      size.
    """
    # from subprocess import Popen, PIPE
    if "stdout" not in kwargs:
        kwargs["stdout"] = PIPE
    if "stderr" not in kwargs:
        kwargs["stderr"] = PIPE
    if folder is not None:
        kwargs["cwd"] = folder

    if venv is not None: # pragma: no cover No guarantee that virtual
                         # envs exist on testing machine.
        prefix = path.dirname(sys.executable)
        if isinstance(venv, str):
            base_home = environ.get("WORKON_HOME")
            if base_home is not None:
                venv_home = path.abspath(path.expanduser(base_home))
                prefix = path.join(venv_home, venv, "bin")
        args[0] = path.join(prefix, args[0])

    oldvars = {}
    if env_vars is not None:
        for name, val in env_vars.items():
            oldvars[name] = environ[name] if name in environ else None
            environ[name] = val

    msg.std("Executing `{}` in {}.".format(' '.join(args), folder), 2)
    pexec = Popen(' '.join(args), shell=shell, executable="/bin/bash", **kwargs)

    if wait:
        sout, serr = pexec.communicate()
    else:
        sout, serr = b"", b""

    if env_vars is not None:
        #Set the environment variables back to what they used to be.
        for name, val in oldvars.items():
            if val is None:
                del environ[name]
            else:
                environ[name] = val

    #Redirect the output and errors so that we don't pollute stdout.
    output = None
    if kwargs["stdout"] is PIPE:
        output = []
        for line in sout.decode("ascii").split('\n'):
            #Filter non fatal exceptions such as future warnings. A full list can be found here
            # https://docs.python.org/3/library/exepctions.html#exception-hierarchy
            if line.strip() == "":
                continue

            if not ("FutureWarning" in line or "import" in line or "\x1b[0m" in line):
                output.append(line)
                if len(output) >= nlines:
                    break

    error = None
    if kwargs["stderr"] is PIPE and serr != '':
        error = []
        for line in serr.strip().decode("ascii").split('\n'):
            if (errignore is None or errignore not in line) and line.strip() != "":
                error.append(line)
            if len(error) >= nlines:
                break

        if printerr and len(error) > 0 and all([isinstance(i, str) for i in error]):
            msg.err(''.join(error))

    return {
        "process": pexec,
        "output": output,
        "error": error
    }


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


def keyhash(key):
    """Returns the bytes of the b85-encoded SHA256 hash for the key so that key
    sizes are standardized.

    Args:
        key (str): key string of arbitrary length.

    Returns:
        bytes: the SHA256 digest of `key`.
    """
    sha = sha256()
    sha.update(key.encode("utf8"))
    
    return base64.b85encode(sha.digest())