# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Argument parsing, the shared command context, and exit-code mapping."""

import argparse
import os
import sys

from ducktests_remote import __version__
from ducktests_remote.cluster import load_extra_hosts, load_nodes, select_nodes
from ducktests_remote.config import ConfigError, expand_path, load_config, set_dotted
from ducktests_remote.globals_builder import Redactor
from ducktests_remote.transport import TransportError, build_transport

EXIT_OK = 0
EXIT_USAGE = 1
EXIT_PREFLIGHT = 2
EXIT_BUSY = 3           # reserved: this deployment has a single runner and no leases
EXIT_TESTS_FAILED = 4
EXIT_TRANSPORT = 5
EXIT_INTERRUPTED = 130

_COLORS = {"red": "\033[31m", "green": "\033[32m", "yellow": "\033[33m",
           "blue": "\033[34m", "bold": "\033[1m", "dim": "\033[2m"}
_RESET = "\033[0m"


class Console:
    """Everything the CLI prints goes through here, so redaction cannot be bypassed."""

    def __init__(self, *, verbose=False, quiet=False, color=True, redactor=None):
        self.verbose = verbose
        self.quiet = quiet
        self.color = color and sys.stdout.isatty() and os.environ.get("TERM") != "dumb"
        self.redactor = redactor or Redactor()

    def paint(self, text, style):
        """:return: ``text`` wrapped in an ANSI style when colour is enabled."""
        if not self.color or style not in _COLORS:
            return text
        return "%s%s%s" % (_COLORS[style], text, _RESET)

    def out(self, message=""):
        """Print a line of normal output."""
        print(self.redactor.redact(message))

    def info(self, message):
        """Print a line suppressed by ``--quiet``."""
        if not self.quiet:
            self.out(message)

    def detail(self, message):
        """Print a line shown only with ``--verbose``."""
        if self.verbose and not self.quiet:
            self.out(self.paint(message, "dim"))

    def warn(self, message):
        """Print a warning to stderr."""
        print(self.redactor.redact(self.paint("WARN  " + message, "yellow")), file=sys.stderr)

    def error(self, message):
        """Print an error to stderr."""
        print(self.redactor.redact(self.paint("ERROR " + message, "red")), file=sys.stderr)

    def heading(self, message):
        """Print a section heading."""
        if not self.quiet:
            self.out("")
            self.out(self.paint(message, "bold"))


class Context:  # pylint: disable=too-many-instance-attributes
    """Config, transports and console, shared by every command."""

    def __init__(self, config, args, console):
        self.config = config
        self.args = args
        self.console = console
        self.dry_run = bool(getattr(args, "dry_run", False))
        self.jobs = int(config.get("jobs", 16))
        self._runner = None
        self._workers = {}

    # -- inventory ---------------------------------------------------------------

    @property
    def cluster_cfg(self):
        """:return: the ``cluster`` config section."""
        return self.config["cluster"]

    @property
    def nodes(self):
        """:return: worker nodes, truncated by ``--num-nodes`` when given."""
        nodes = load_nodes(self.cluster_cfg)
        return select_nodes(nodes, getattr(self.args, "num_nodes", None))

    @property
    def all_nodes(self):
        """:return: worker nodes plus ``extra_hosts`` (provision/deploy/clean/doctor)."""
        return self.nodes + load_extra_hosts(self.cluster_cfg)

    @property
    def runner_host(self):
        """:return: the host ducktape will run on."""
        return self.cluster_cfg.get("runner") or "local"

    @property
    def state_root(self):
        """:return: the runner-side state root, ``~`` unexpanded."""
        return self.cluster_cfg.get("state_root") or "~/.ducktests-remote"

    @property
    def identity_file(self):
        """:return: the ssh identity path *as the runner sees it*."""
        return self.cluster_cfg.get("identity_file")

    # -- transports --------------------------------------------------------------

    @property
    def runner(self):
        """:return: a transport to the runner, created once."""
        if self._runner is None:
            self._runner = build_transport(
                self.runner_host,
                user=self.cluster_cfg.get("user") if self.runner_host != "local" else None,
                port=self.cluster_cfg.get("port", 22),
                identity_file=self._coordinator_identity(),
                connect_timeout=self.config["ssh"]["connect_timeout"],
                dry_run=self.dry_run, verbose=self.console.verbose,
                printer=self.console.out)
        return self._runner

    def worker(self, node):
        """:return: a transport to one worker, created once per host."""
        if node.host not in self._workers:
            self._workers[node.host] = build_transport(
                node.host, user=node.user, port=node.port,
                identity_file=self._coordinator_identity(node.identity_file),
                connect_timeout=self.config["ssh"]["connect_timeout"],
                dry_run=self.dry_run, verbose=self.console.verbose,
                printer=self.console.out)
        return self._workers[node.host]

    def _coordinator_identity(self, identity=None):
        """
        The configured ``identity_file`` is a runner-side path; it may or may not name a
        file that exists here.  Use it when it does, otherwise let the system ssh client
        fall back to ``~/.ssh/config`` and the agent.
        """
        candidate = expand_path(identity or self.identity_file)
        if candidate and os.path.isfile(candidate):
            return candidate
        return None

    def state_root_resolved(self):
        """:return: the state root with ``~`` expanded against the runner's home."""
        return self.runner.expand(self.state_root)


def _common_parser():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", action="append", default=argparse.SUPPRESS, metavar="FILE",
                        help="configuration file (YAML or JSON); repeatable, applied in order")
    parser.add_argument("--profile", action="append", default=argparse.SUPPRESS, metavar="NAME",
                        help="named profile from profiles_dir; repeatable, applied in order")
    parser.add_argument("--runner", default=argparse.SUPPRESS, metavar="HOST",
                        help="host running ducktape, or 'local'; overrides the config")
    parser.add_argument("--jobs", type=int, default=argparse.SUPPRESS, metavar="N",
                        help="fan-out parallelism (default 16)")
    parser.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS,
                        help="show every command and per-host output")
    parser.add_argument("-q", "--quiet", action="store_true", default=argparse.SUPPRESS,
                        help="only print results and errors")
    parser.add_argument("--dry-run", action="store_true", default=argparse.SUPPRESS,
                        help="print every command and generated file, execute nothing")
    parser.add_argument("--no-color", action="store_true", default=argparse.SUPPRESS,
                        help="disable ANSI colour")
    parser.add_argument("--fail-fast", action="store_true", default=argparse.SUPPRESS,
                        help="abort a fan-out after the first host failure")
    return parser


def build_parser():
    """:return: the fully wired argument parser."""
    common = _common_parser()
    parser = argparse.ArgumentParser(
        prog="ducktests-remote", parents=[common],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Run Apache Ignite ducktests against a real VM cluster.",
        epilog="Start with `ducktests-remote doctor` and `--dry-run`.")
    parser.add_argument("--version", action="version", version="ducktests-remote " + __version__)
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    # Imported here so `ducktests_remote.cli` stays importable without the command
    # modules having been loaded, which keeps the import-guard test cheap.
    from ducktests_remote.commands import (  # pylint: disable=import-outside-toplevel
        clean, deploy, doctor, fetch, keys, logs, provision, run, status, stop)

    for module in (run, status, logs, fetch, stop, provision, deploy, clean, doctor, keys):
        module.register(subparsers, common)

    return parser


def _flag_overrides(args):
    """:return: a config overlay built from the explicit command-line flags."""
    overlay = {}
    if getattr(args, "runner", None):
        set_dotted(overlay, "cluster.runner", args.runner)
    if getattr(args, "jobs", None):
        set_dotted(overlay, "jobs", int(args.jobs))
    for flag, dotted in (("install_root", "cluster.install_root"),
                         ("state_root", "cluster.state_root"),
                         ("dist_dir", "deploy.dist_dir"),
                         ("source_root", "run.source_root"),
                         ("work_dir", "run.work_dir")):
        value = getattr(args, flag, None)
        if value:
            set_dotted(overlay, dotted, value)
    return overlay


def split_passthrough(argv):
    """
    Split ``-- <extra ducktape args>`` off the end of the command line.

    argparse cannot express "everything after the first bare ``--`` belongs to another
    program" without swallowing legitimate arguments, so it is done up front.
    """
    if "--" not in argv:
        return argv, []
    index = argv.index("--")
    return argv[:index], argv[index + 1:]


def main(argv=None):
    """CLI entry point. :return: the process exit code."""
    argv = list(sys.argv[1:] if argv is None else argv)
    argv, passthrough = split_passthrough(argv)

    parser = build_parser()
    args = parser.parse_args(argv)
    args.passthrough = passthrough

    if not getattr(args, "command", None):
        parser.print_help()
        return EXIT_USAGE

    console = Console(verbose=getattr(args, "verbose", False),
                      quiet=getattr(args, "quiet", False),
                      color=not getattr(args, "no_color", False))
    try:
        config = load_config(config_files=getattr(args, "config", None) or [],
                             profiles=getattr(args, "profile", None) or [],
                             overrides=_flag_overrides(args))
        ctx = Context(config, args, console)
        return args.handler(ctx)
    except ConfigError as ex:
        console.error(str(ex))
        return EXIT_USAGE
    except TransportError as ex:
        console.error(str(ex))
        if console.verbose:
            raise
        return EXIT_TRANSPORT
    except KeyboardInterrupt:
        console.out("")
        console.error("interrupted")
        return EXIT_INTERRUPTED
    except BrokenPipeError:
        return EXIT_OK
