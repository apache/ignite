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

"""``run`` - compose the artifacts, launch ducktape detached, and follow its log."""

import json
import os
import platform
import posixpath
import shlex
import socket
import sys
import time
from pathlib import Path

from ducktests_remote import (__version__, cluster as cluster_mod, globals_builder, pipconf,
                              runs)
from ducktests_remote.cli import (EXIT_OK, EXIT_PREFLIGHT, EXIT_TESTS_FAILED, Console)
from ducktests_remote.commands import doctor
from ducktests_remote.config import ConfigError, expand_path
from ducktests_remote.progress import build_progress

DEFAULT_EXCLUDES = [".git", "target", "results", "__pycache__", "*.pyc", ".idea",
                    "venv", ".venv", "*.egg-info", ".tox", ".pytest_cache"]

IGNITE_IGNORE_FILE = ".ducktestsignore"

# Where the ducktests live inside an Ignite checkout, and the file that proves it is one.
TESTS_SUBDIR = ("modules", "ducktests", "tests")
CHECKOUT_MARKER = ("docker", "requirements.txt")

FOLLOW_POLL_SEC = 1.5
DOUBLE_INTERRUPT_SEC = 3.0

# run.sh exits with this when the requirements file it would install from is absent.
MISSING_REQUIREMENTS_EXIT = 3


def register(subparsers, common):
    """Wire up the ``run`` subcommand."""
    parser = subparsers.add_parser(
        "run", parents=[common], help="launch a ducktape run on the cluster",
        description="Compose the configuration, generate cluster.json / globals.json / "
                    "run.sh on the runner, and launch ducktape detached.")
    parser.add_argument("test_paths", nargs="*", metavar="TEST_PATH",
                        help="test paths, relative to modules/ducktests/tests as in the "
                             "Docker flow (./ignitetest/tests/smoke_test.py::Cls.method); "
                             "paths relative to the checkout root also work")
    parser.add_argument("-t", "--tc-path", action="append", default=[], metavar="PATH",
                        help="test path; repeatable, equivalent to a positional argument")
    parser.add_argument("-g", "--global", action="append", default=[], dest="globals_kv",
                        metavar="KEY=VALUE",
                        help="dotted globals override, e.g. -g ssl.enabled=true")
    parser.add_argument("-p", "--param", action="append", default=[], dest="params_kv",
                        metavar="KEY=VALUE", help="dotted --parameters override")
    parser.add_argument("--globals-json", metavar="JSON",
                        help="raw globals base layer; paste an existing Jenkins blob here")
    parser.add_argument("--globals-file", metavar="PATH", help="raw globals base layer from file")
    parser.add_argument("--params-json", metavar="JSON", help="raw --parameters base layer")
    parser.add_argument("--cluster-file", metavar="PATH",
                        help="use this coordinator-side cluster file verbatim instead of "
                             "generating one from the inventory")
    parser.add_argument("-n", "--num-nodes", type=int, default=None, metavar="N",
                        help="use the first N inventory hosts (default: all)")
    parser.add_argument("--source-root", metavar="PATH",
                        help="Ignite checkout synced to the runner (default: the checkout "
                             "containing the current directory)")
    parser.add_argument("--no-sync", action="store_true",
                        help="assume the sources are already on the runner")
    parser.add_argument("--exclude", action="append", default=[], metavar="PATTERN",
                        help="extra sync exclusion; repeatable")
    parser.add_argument("--work-dir", metavar="PATH",
                        help="runner-side Ignite checkout to run from; ducktape itself "
                             "runs in its modules/ducktests/tests directory "
                             "(default: the synced source root)")
    parser.add_argument("--install-sources", action="store_true",
                        help="pip install the synced sources into the runner venv. Not needed "
                             "for test discovery; ducktape puts the sources on sys.path itself")
    parser.add_argument("--pip-index-url", metavar="URL",
                        help="package index for the runner venv, when PyPI is unreachable")
    parser.add_argument("--pip-extra-index-url", action="append", default=None, metavar="URL",
                        help="additional index; repeatable, replaces the configured list")
    parser.add_argument("--pip-trusted-host", action="append", default=None, metavar="HOST",
                        help="host whose certificate pip should not verify; repeatable")
    parser.add_argument("--pip-timeout", type=int, default=None, metavar="SECONDS",
                        help="pip socket timeout, for a slow internal mirror")
    parser.add_argument("--pip-cert", metavar="PATH",
                        help="CA bundle for the index, as a RUNNER-side path")
    parser.add_argument("--repeat", type=int, default=None, metavar="N")
    parser.add_argument("--max-parallel", type=int, default=None, metavar="N")
    parser.add_argument("--test-runner-timeout", type=int, default=None, metavar="MS")
    parser.add_argument("--results-root", metavar="PATH",
                        help="runner-side results root (default: <run dir>/results)")
    parser.add_argument("--skip-preflight", action="store_true", help="skip the doctor checks")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--follow", dest="follow", action="store_true", default=True,
                       help="stream the log until the run ends (default)")
    group.add_argument("--detach", dest="follow", action="store_false",
                       help="print the run id and exit immediately")
    parser.set_defaults(handler=execute)


def execute(ctx):  # pylint: disable=too-many-return-statements
    """Launch a run. :return: the process exit code."""
    args = ctx.args
    console = ctx.console

    raw_test_paths = list(args.test_paths) + list(args.tc_path)
    if not raw_test_paths:
        raise ConfigError("no tests given; pass a path positionally or with -t/--tc-path")

    source_root = _source_root(ctx)
    _check_source_root(ctx, source_root)

    composed, params = _compose_payloads(ctx)

    nodes = ctx.nodes
    cluster_payload, cluster_source, cluster_text = _cluster_payload(ctx, nodes)
    _warn_about_topology(ctx, nodes, raw_test_paths)

    if not args.skip_preflight and not ctx.dry_run:
        console.heading("PREFLIGHT")
        checks, diagnoses = doctor.run_checks(ctx, _versions(composed))
        doctor.print_report(ctx, checks, diagnoses)
        if doctor.has_failures(checks):
            console.error("preflight failed; fix the FAIL rows above or pass --skip-preflight")
            return EXIT_PREFLIGHT

    run_id = runs.new_run_id(_coordinator_user())
    state_root = ctx.runner.expand(ctx.state_root)
    paths = runs.RunPaths(state_root, run_id)

    work_dir = _work_dir(ctx, paths, source_root)
    results_root = args.results_root or paths.results_dir
    test_paths = _test_paths(ctx, raw_test_paths, source_root, work_dir)

    run_sh = runs.render_run_script(
        version=__version__, timestamp=runs.utc_now_iso(),
        author="%s@%s" % (_coordinator_user(), socket.gethostname()),
        cwd=_tests_dir(work_dir), results_root=results_root,
        cluster_file=paths.cluster_file, globals_file=paths.globals_file,
        test_paths=test_paths, venv=_venv_path(ctx),
        parameters_file=paths.parameters_file if params else None,
        repeat=args.repeat, max_parallel=args.max_parallel,
        test_runner_timeout=args.test_runner_timeout,
        extra_args=getattr(args, "passthrough", []))

    _print_artifacts(ctx, paths, cluster_payload, composed, params, run_sh, cluster_source)

    if ctx.dry_run:
        console.out("")
        console.info("--dry-run: nothing was created, uploaded or started.")
        return EXIT_OK

    console.heading("PREPARING %s" % run_id)
    ctx.runner.mkdirs(paths.run_dir)
    ctx.runner.mkdirs(paths.results_dir)

    if not args.no_sync:
        _sync_sources(ctx, source_root, paths)
    _ensure_venv(ctx, work_dir)
    if args.install_sources:
        _install_sources(ctx, work_dir)

    ctx.runner.write_file(cluster_text or cluster_mod.dumps(cluster_payload),
                          paths.cluster_file)
    ctx.runner.write_file(globals_builder.dumps(composed), paths.globals_file, mode=0o600)
    if params:
        ctx.runner.write_file(globals_builder.dumps(params), paths.parameters_file, mode=0o600)
    ctx.runner.write_file(run_sh, paths.run_script, mode=0o755)
    ctx.runner.write_file(runs.render_launch_script(paths), paths.launch_script, mode=0o755)

    runs.write_meta(ctx.runner, paths,
                    _meta(ctx, run_id, test_paths, nodes, work_dir, results_root))
    _update_latest_link(ctx, paths)

    pid = ctx.runner.run_script(runs.render_detach_script(paths)).check().out
    console.out("")
    console.out("run id : %s" % run_id)
    console.out("runner : %s (pid %s)" % (ctx.runner_host, pid))
    console.out("run dir: %s" % paths.run_dir)
    console.out("log    : %s" % paths.log_file)

    if not args.follow:
        _print_reattach(console, run_id)
        return EXIT_OK

    return _follow(ctx, paths, run_id)


# -- composition -------------------------------------------------------------------


def _compose_payloads(ctx):
    args = ctx.args
    redactor = ctx.console.redactor

    layers = []
    raw = globals_builder.load_raw_layer(args.globals_json, args.globals_file)
    if raw is not None:
        layers.append(("--globals-json/--globals-file", raw))
    layers.append(("config globals", ctx.config.get("globals") or {}))
    composed, _ = globals_builder.build(layers, args.globals_kv, redactor=redactor)

    param_layers = []
    raw_params = globals_builder.load_raw_layer(args.params_json, None)
    if raw_params is not None:
        param_layers.append(("--params-json", raw_params))
    param_layers.append(("config parameters", ctx.config.get("parameters") or {}))
    params, _ = globals_builder.build(param_layers, args.params_kv, redactor=redactor)

    return composed, params


def _cluster_payload(ctx, nodes):
    """
    :return: ``(parsed, source, verbatim_text)``.

    ``--cluster-file`` is uploaded byte for byte - it is the migration path from a
    hand-written file such as the current ``49_cluster.json``, and rewriting it would
    silently drop anything the CLI does not model. It is still parsed here so that a
    broken file fails now rather than inside ducktape.
    """
    if ctx.args.cluster_file:
        path = Path(expand_path(ctx.args.cluster_file))
        if not path.is_file():
            raise ConfigError("cluster file not found: %s" % path)
        text = path.read_text(encoding="utf-8")
        try:
            parsed = json.loads(text)
        except ValueError as ex:
            raise ConfigError("%s is not valid JSON: %s" % (path, ex)) from ex
        if not parsed.get("nodes"):
            raise ConfigError("%s has no 'nodes' entry" % path)
        return parsed, str(path), text
    identity = ctx.runner.expand(ctx.identity_file) if ctx.identity_file else None
    return cluster_mod.cluster_json(nodes, identity_file=identity), "inventory", None


def _warn_about_topology(ctx, nodes, test_paths):
    console = ctx.console
    if ctx.runner_host not in ("local", None):
        if any(node.host == ctx.runner_host for node in nodes):
            console.warn("the runner %s is also listed as a worker. The Docker flow reserves "
                         "ducker01 for ducktape; on a real cluster that is your call, but "
                         "ducktape will schedule Ignite nodes onto the machine it runs on."
                         % ctx.runner_host)
    if len(nodes) < 3 and len(test_paths) > 0:
        console.warn("only %d worker%s in the inventory. Most ignitetest suites declare "
                     "@cluster(num_nodes=...) well above that and will be skipped as "
                     "un-runnable." % (len(nodes), "" if len(nodes) == 1 else "s"))


def _versions(composed):
    versions = composed.get("ignite_versions")
    if isinstance(versions, str):
        return [versions]
    return list(versions or [])


# -- runner-side preparation --------------------------------------------------------


def _source_root(ctx):
    """
    :return: the Ignite checkout to sync, as a coordinator-side path.

    An explicit ``--source-root`` / ``run.source_root`` is taken as given.  Otherwise the
    current directory is walked upwards until a checkout is found, so the command works
    from ``modules/ducktests/tests`` - where ``./docker/run_tests.sh`` is run from - as
    well as from the repository root.
    """
    configured = ctx.args.source_root or ctx.config["run"].get("source_root")
    if configured:
        return Path(expand_path(configured)).resolve()
    cwd = Path(os.getcwd()).resolve()
    for candidate in (cwd,) + tuple(cwd.parents):
        if is_ignite_checkout(candidate):
            return candidate
    return cwd


def is_ignite_checkout(path):
    """:return: True when ``path`` is the root of an Ignite source tree."""
    return Path(path).joinpath(*TESTS_SUBDIR).joinpath(*CHECKOUT_MARKER).is_file()


def _check_source_root(ctx, source_root):
    """
    Reject a source root that is not an Ignite checkout, before anything is uploaded.

    Getting this wrong is easy - the state root and the tests directory are both plausible
    places to stand - and every symptom of it appears much later, as a missing
    requirements file or as ducktape finding no tests.
    """
    if is_ignite_checkout(source_root):
        return
    if ctx.args.no_sync and not source_root.exists():
        return          # a runner-side path that this machine cannot see; nothing to check
    raise ConfigError(
        "%s is not an Ignite checkout: it has no %s.\nThe whole source tree is synced to "
        "the runner and ducktape is run from its %s directory, so this must be the "
        "repository root (the directory that contains `modules/`). Run the command from "
        "anywhere inside the checkout, or pass --source-root / set run.source_root."
        % (source_root, posixpath.join(*TESTS_SUBDIR, *CHECKOUT_MARKER),
           posixpath.join(*TESTS_SUBDIR)))


def _tests_dir(work_dir):
    """:return: the runner-side directory ducktape runs from."""
    return posixpath.join(work_dir, *TESTS_SUBDIR)


def _test_paths(ctx, raw_paths, source_root, work_dir):
    """
    Rewrite the given test paths into the form ducktape sees on the runner.

    ducktape's working directory is the tests directory, the same one
    ``./docker/run_tests.sh`` uses, so ``./ignitetest/tests/smoke_test.py`` means here
    what it means locally.  A path is accepted in whatever form resolves on the
    coordinator - relative to the current directory, to the tests directory, or to the
    checkout root - and is rewritten from there.
    """
    tests_local = source_root.joinpath(*TESTS_SUBDIR)
    return [_one_test_path(ctx, raw, source_root, tests_local, work_dir)
            for raw in raw_paths]


def _one_test_path(ctx, raw, source_root, tests_local, work_dir):
    body, sep, suffix = raw.partition("::")
    resolved = _resolve_test_file(body, source_root, tests_local)

    if resolved is None:
        # Legitimate under --no-sync against a checkout this machine does not have, and
        # ducktape gives a better message than a guess would.
        ctx.console.warn("test path %s does not exist on this machine; passing it to "
                         "ducktape unchanged" % body)
        return raw

    try:
        return "./%s%s%s" % (resolved.relative_to(tests_local).as_posix(), sep, suffix)
    except ValueError:
        pass

    try:
        rel = resolved.relative_to(source_root)
    except ValueError:
        ctx.console.warn("test path %s is outside the source root %s, so it is not synced; "
                         "it will only run if that exact path exists on the runner"
                         % (resolved, source_root))
        return raw
    return "%s%s%s" % (posixpath.join(work_dir, rel.as_posix()), sep, suffix)


def _resolve_test_file(body, source_root, tests_local):
    """:return: the coordinator-side file ``body`` names, or None if it names none."""
    expanded = Path(expand_path(body))
    if expanded.is_absolute():
        return expanded.resolve() if expanded.exists() else None
    for base in (Path(os.getcwd()), tests_local, source_root):
        candidate = base / expanded
        if candidate.exists():
            return candidate.resolve()
    return None


def _work_dir(ctx, paths, source_root):
    configured = ctx.args.work_dir or ctx.config["run"].get("work_dir")
    if configured:
        return ctx.runner.expand(configured)
    if ctx.args.no_sync:
        # Nothing was uploaded, so the sources must already be where the operator says.
        return ctx.runner.expand(str(source_root).replace(os.sep, "/"))
    return paths.src_dir


def _excludes(ctx, source_root):
    if ctx.args.exclude:
        return list(ctx.args.exclude)
    ignore_file = Path(source_root) / IGNITE_IGNORE_FILE
    if ignore_file.is_file():
        return [line.strip() for line in ignore_file.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.strip().startswith("#")]
    return list(DEFAULT_EXCLUDES) + list(ctx.config["run"].get("exclude") or [])


def _sync_sources(ctx, source_root, paths):
    excludes = _excludes(ctx, source_root)
    size_mb = _payload_size_mb(source_root, excludes)
    limit = int(ctx.config["run"]["max_payload_mb"])
    ctx.console.info("syncing %s (%.1f MB) -> %s:%s"
                     % (source_root, size_mb, ctx.runner_host, paths.src_dir))
    if size_mb > limit:
        raise ConfigError(
            "source payload is %.1f MB, above the %d MB limit. That almost always means a "
            "build directory leaked into the payload - check --exclude / %s. Distributions "
            "belong in `ducktests-remote deploy`, never in the source sync."
            % (size_mb, limit, IGNITE_IGNORE_FILE))
    total = int(size_mb * 1024 * 1024)
    progress = build_progress(ctx, [ctx.runner_host])
    watcher = None
    if progress.watching:
        def watcher(sent, fraction):  # noqa: F811 - reported only when something draws it
            progress.sent(ctx.runner_host, sent, total, fraction=fraction)
    with progress:
        progress.phase(ctx.runner_host, "sending")
        ctx.runner.upload_dir(source_root, paths.src_dir, excludes=excludes,
                              on_progress=watcher)
        progress.done(ctx.runner_host)


def _payload_size_mb(source_root, excludes):
    from ducktests_remote.transport import is_excluded  # pylint: disable=import-outside-toplevel
    total = 0
    root = Path(source_root)
    for entry in root.rglob("*"):
        if entry.is_dir() or entry.is_symlink():
            continue
        if is_excluded(entry.relative_to(root), excludes):
            continue
        try:
            total += entry.stat().st_size
        except OSError:
            continue
    return total / (1024.0 * 1024.0)


def _venv_path(ctx):
    """
    :return: the runner-side venv path, or None when ducktape is expected on PATH.

    The environment is controlled rather than assumed: an explicitly configured path is
    used as given, otherwise the default under the state root is used and created when
    it does not exist yet.
    """
    configured = ctx.config["runner"].get("venv")
    if configured:
        return ctx.runner.expand(configured)
    if not ctx.config["runner"].get("create_venv", True):
        return None
    return posixpath.join(ctx.runner.expand(ctx.state_root), "venv")


def _ensure_venv(ctx, work_dir):
    """Create the runner venv and install the pinned requirements when it is missing."""
    venv = _venv_path(ctx)
    if not venv:
        return
    python = ctx.config["runner"].get("python", "python3")
    requirements = ctx.config["runner"].get("requirements") or posixpath.join(
        _tests_dir(work_dir), *CHECKOUT_MARKER)
    index_arg = pipconf.pip_args_str(ctx.config)

    script = """set -eu
venv=%(venv)s
req=%(req)s
if [ ! -x "$venv/bin/python3" ]; then
  echo "creating venv at $venv"
  %(python)s -m venv "$venv"
  created=1
fi
if ! "$venv/bin/python3" -c 'import ducktape' 2>/dev/null; then
  if [ -f "$req" ]; then
    echo "installing $req into $venv"
    "$venv/bin/pip3" install --disable-pip-version-check %(index)s -r "$req"
  else
    echo "MISSING_REQUIREMENTS $req" >&2
    exit 3
  fi
fi
"$venv/bin/python3" -c 'import ducktape;print("ducktape " + ducktape.__version__)'
""" % {"venv": shlex.quote(venv), "req": shlex.quote(requirements),
       "python": shlex.quote(python), "index": index_arg}

    result = ctx.runner.run_script(script, check=False)
    if result.returncode == MISSING_REQUIREMENTS_EXIT:
        # Nothing was installed and nothing was reached: the file simply is not there.
        raise ConfigError(
            "the runner venv at %s has no ducktape, and the requirements file to install "
            "it from does not exist on the runner:\n  %s\nThat is <source root>/%s, so it "
            "normally means the synced tree is not an Ignite checkout. Check --source-root, "
            "or set runner.requirements to a file the runner does have."
            % (venv, requirements, posixpath.join(*TESTS_SUBDIR, *CHECKOUT_MARKER)))
    if not result.ok:
        raise ConfigError(
            "could not prepare the runner venv at %s:\n%s\nInstalling %s from: %s.\nEither "
            "point runner.venv at an existing environment, or set pip.index_url to an "
            "index the runner can reach."
            % (venv, (result.stderr or result.stdout).strip(), requirements,
               pipconf.describe(ctx.config, ctx.console.redactor)))
    ctx.console.info(result.out.splitlines()[-1] if result.out else "venv ready")


def _install_sources(ctx, work_dir):
    venv = _venv_path(ctx)
    pip = posixpath.join(venv, "bin", "pip3") if venv else "pip3"
    tests_dir = _tests_dir(work_dir)
    ctx.console.info("installing sources from %s" % tests_dir)
    ctx.runner.run([pip, "install", "--disable-pip-version-check"]
                   + pipconf.pip_args(ctx.config) + ["-e", tests_dir]).check()


def _update_latest_link(ctx, paths):
    ctx.runner.run_script("set -eu\nln -sfn %s %s\n"
                          % (shlex.quote(paths.run_dir), shlex.quote(paths.latest_link)),
                          check=False)


def _meta(ctx, run_id, test_paths, nodes, work_dir, results_root):
    from ducktests_remote.config import redacted_summary  # pylint: disable=import-outside-toplevel
    return {
        "run_id": run_id,
        "cli_version": __version__,
        "coordinator": {"user": _coordinator_user(), "host": socket.gethostname(),
                        "platform": platform.platform()},
        "runner": ctx.runner_host,
        "cluster": ctx.cluster_cfg.get("name"),
        "nodes": [n.host for n in nodes],
        "test_paths": list(test_paths),
        "work_dir": work_dir,
        "results_root": results_root,
        "started_at": runs.utc_now_iso(),
        "started_epoch": time.time(),
        "config": ctx.console.redactor.redact_structure(redacted_summary(ctx.config)),
    }


# -- output ------------------------------------------------------------------------


def _print_artifacts(ctx, paths, cluster_payload, composed, params, run_sh, cluster_source):
    console = ctx.console
    if not (ctx.dry_run or console.verbose):
        return
    console.heading("cluster.json (from %s) -> %s" % (cluster_source, paths.cluster_file))
    console.out(cluster_mod.dumps(cluster_payload).rstrip())
    console.heading("globals.json (redacted) -> %s  [mode 0600]" % paths.globals_file)
    console.out(globals_builder.dumps(
        console.redactor.redact_structure(composed)).rstrip())
    if params:
        console.heading("parameters.json -> %s" % paths.parameters_file)
        console.out(globals_builder.dumps(
            console.redactor.redact_structure(params)).rstrip())
    console.heading("run.sh -> %s" % paths.run_script)
    console.out(run_sh.rstrip())


def _print_reattach(console: Console, run_id):
    console.out("")
    console.out("The run continues on the runner. To come back to it:")
    console.out("    ducktests-remote logs %s -f" % run_id)
    console.out("    ducktests-remote status %s" % run_id)
    console.out("To stop it and clean the workers:")
    console.out("    ducktests-remote stop %s" % run_id)


# -- follow ------------------------------------------------------------------------


def _follow(ctx, paths, run_id):
    """
    Stream the runner's log until the process ends.

    Ctrl-C detaches; it never kills the run.  Losing a three hour run to a reflex is not
    a recoverable mistake, so stopping takes a deliberate second interrupt.
    """
    console = ctx.console
    console.heading("FOLLOWING %s  (Ctrl-C detaches, it does not stop the run)" % run_id)
    offset = 1
    last_interrupt = 0.0

    while True:
        try:
            offset = _drain(ctx, paths, offset, console)

            state = runs.read_state(ctx.runner, paths)
            if state.exit_code is not None:
                # The process wrote its exit code between the two reads above, so drain
                # once more or the last lines of the session report are lost.
                _drain(ctx, paths, offset, console)
                return _finish(ctx, paths, run_id, state)
            time.sleep(FOLLOW_POLL_SEC)
        except KeyboardInterrupt:
            now = time.monotonic()
            if now - last_interrupt < DOUBLE_INTERRUPT_SEC:
                return _offer_stop(ctx, paths, run_id)
            last_interrupt = now
            console.out("")
            console.out("Detaching. The run keeps going on %s." % ctx.runner_host)
            _print_reattach(console, run_id)
            console.out("")
            console.out("Press Ctrl-C again within %ds to stop the run instead."
                        % int(DOUBLE_INTERRUPT_SEC))


def _drain(ctx, paths, offset, console):
    """Print whatever the log has gained since ``offset``. :return: the new offset."""
    chunk = ctx.runner.run(["tail", "-c", "+%d" % offset, paths.log_file], check=False)
    if chunk.stdout:
        offset += len(chunk.stdout.encode("utf-8"))
        sys.stdout.write(console.redactor.redact(chunk.stdout))
        sys.stdout.flush()
    return offset


def _offer_stop(ctx, paths, run_id):
    console = ctx.console
    console.out("")
    if not sys.stdin.isatty():
        console.out("Detached (no terminal to confirm on). Run `ducktests-remote stop %s` "
                    "to stop it." % run_id)
        return EXIT_OK
    try:
        answer = input("Stop run %s and clean the workers? [y/N] " % run_id)
    except (EOFError, KeyboardInterrupt):
        answer = ""
    if answer.strip().lower() in ("y", "yes"):
        from ducktests_remote.commands import stop  # pylint: disable=import-outside-toplevel
        return stop.stop_run(ctx, paths, timeout=60, kill=False, clean=True)
    console.out("Left running. `ducktests-remote logs %s -f` to reattach." % run_id)
    return EXIT_OK


def _finish(ctx, paths, run_id, state):
    console = ctx.console
    console.out("")
    console.out("run %s %s after %s (ducktape exit %s)"
                % (run_id, state.state, runs.format_duration(state.elapsed), state.exit_code))
    console.out("results: %s" % posixpath.join(paths.results_dir, "latest"))
    console.out("fetch  : ducktests-remote fetch %s" % run_id)
    return EXIT_OK if state.exit_code == 0 else EXIT_TESTS_FAILED


def _coordinator_user():
    return os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
