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

"""
Configuration discovery, layering and validation.

Layers, later overriding earlier, dicts deep-merged and lists replaced wholesale:

1. built-in defaults
2. ``~/.ducktests-remote/config.yaml``
3. ``--config FILE`` (repeatable, in order)
4. ``--profile NAME`` (repeatable, in order)
5. ``DTR_*`` environment variables
6. explicit command-line flags
7. ``-g KEY=VALUE`` dotted overrides (globals only, applied by :mod:`globals_builder`)
"""

import copy
import difflib
import getpass
import json
import os
import re
from pathlib import Path

import yaml

USER_CONFIG_DIR = Path("~/.ducktests-remote").expanduser()
USER_CONFIG_FILE = USER_CONFIG_DIR / "config.yaml"
PACKAGE_EXAMPLES = Path(__file__).resolve().parent / "examples"

# Pinned requirements the runner venv must satisfy.  Read from the repo rather than
# hardcoded so the ducktape pin has exactly one source of truth.
REQUIREMENTS_RELPATH = "modules/ducktests/tests/docker/requirements.txt"

# System utilities, derived from modules/ducktests/tests/docker/Dockerfile section 2.
# The Dockerfile is the source of truth; when the two disagree, the Dockerfile wins.
# Image-only entries (openssh-server, vim, mc, build-essential, cmake, libfuse-dev...)
# are deliberately not replicated: a real VM already has an sshd and no build toolchain
# is needed to *run* tests.
DOCKERFILE_PACKAGES = [
    "sudo",
    "netcat-traditional",
    "iptables",
    "rsync",
    "unzip",
    "wget",
    "curl",
    "jq",
    "coreutils",
    "net-tools",
]

# ignitetest shells out to `sudo iptables` only from IgniteAwareService.drop_network
# (services/utils/ignite_aware.py).  Exactly two suites reach it.
SUDO_DEPENDENT_TESTS = (
    "ignitetest/tests/discovery_test.py",
    "ignitetest/tests/cellular_affinity_test.py",
)

# Where a JDK is looked for on a worker, in this order of preference within a major
# version.  A search path may be a directory *of* JDK homes (/usr/lib/jvm) or a JDK home
# itself; both shapes are probed.
JAVA_SEARCH_PATHS = ["/opt", "/usr/lib/jvm", "/usr/java"]

# modules/ducktests/tests/docker/Dockerfile pins `ARG jdk_version="eclipse-temurin:17"`.
# The Dockerfile is the source of truth here too; when the two disagree, it wins.
JAVA_MAJOR = 17

# ignitetest services launch these main classes; see services/ignite.py,
# services/ignite_app.py, services/utils/cdc/ignite_cdc.py and .../kafka_to_ignite.py.
IGNITE_MAIN_CLASSES = (
    "org.apache.ignite.startup.cmdline.CommandLineStartup",
    "org.apache.ignite.startup.cmdline.CdcCommandLineStartup",
    "org.apache.ignite.internal.ducktest.utils.IgniteAwareApplicationService",
    "org.apache.ignite.cdc.kafka.KafkaToIgniteCommandLineStartup",
)

DEFAULTS = {
    "profiles_dir": str(USER_CONFIG_DIR / "profiles"),
    "cluster": {
        "name": "default",
        "identity_file": "~/.ssh/id_rsa",
        "user": None,           # resolved to the coordinator's $USER, never to "ducker"
        "port": 22,
        "install_root": "/opt",
        "runner": "local",
        "state_root": "~/.ducktests-remote",
        "nodes": [],
        "extra_hosts": [],
    },
    "runner": {
        # Deterministic environment: use venv when given, else the default under the
        # state root, creating and populating it when missing.
        "venv": None,
        "python": "python3",
        "create_venv": True,
        "requirements": None,   # defaults to <source_root>/modules/.../requirements.txt
    },
    "run": {
        "source_root": None,
        "work_dir": None,
        "exclude": [],
        "max_payload_mb": 200,
        # ducktape's loader walks up from each test file while __init__.py exists and
        # appends the resulting top-level directory to sys.path
        # (ducktape/tests/loader.py::_add_top_level_dirs_to_sys_path), so the synced
        # sources are importable without being installed.  Only the third-party
        # requirements have to be in the venv.  Left as an opt-in escape hatch.
        "install_sources": False,
    },
    "deploy": {
        "dist_dir": "./dist",
        "install_root": None,   # defaults to cluster.install_root
        "sudo": False,          # /opt is writable by the ordinary user on these clusters
        "owner": None,
        "staging_dir": "/tmp/ducktests-remote-staging",
        "checksum": False,
    },
    "clean": {
        # pgrep -f patterns; see IGNITE_MAIN_CLASSES above.
        "process_pattern": "org.apache.ignite",
        # persistent_root default from ignitetest/services/utils/path.py.
        "paths": ["/mnt/service"],
        "allowed_roots": ["/mnt", "/tmp", "/var/tmp"],
    },
    "provision": {
        "packages": list(DOCKERFILE_PACKAGES),
        "install_jdk": False,
        "ssh_env_path_extra": [],
        "dirs": ["/mnt/service"],
    },
    # Everything pip needs to reach an index that is not PyPI.  Only the runner ever runs
    # pip: the venv install and `run --install-sources`.  Workers are driven over plain
    # ssh and never see Python.
    "pip": {
        "index_url": None,          # --index-url
        "extra_index_url": [],      # --extra-index-url, repeatable; a bare string is ok
        "trusted_host": [],         # --trusted-host, repeatable; a bare string is ok
        "timeout": None,            # --timeout, seconds
        "retries": None,            # --retries
        "cert": None,               # --cert; a RUNNER-side path to a CA bundle
    },
    # Which JVM the workers run the tests under.  See ducktests_remote/java.py for the
    # resolution ladder and for why PATH matters more here than JAVA_HOME.
    "java": {
        "major": JAVA_MAJOR,
        "home": None,               # explicit JDK home on the workers; set -> no search
        "search_paths": list(JAVA_SEARCH_PATHS),
        "archive": None,            # coordinator-side .tar.gz/.tgz/.tar, or a directory
        "install_root": None,       # defaults to cluster.install_root
        "name": None,               # target directory name; defaults to the archive's own
        "ssh_environment": True,    # write ~/.ssh/environment
        "bashrc": True,             # write a marked block at the top of ~/.bashrc
    },
    "ssh": {
        "connect_timeout": 15,
    },
    "jobs": 16,
    "globals": {},
    "parameters": {},
}

_FREE_FORM_SECTIONS = ("globals", "parameters")

_PLACEHOLDER = re.compile(r"\$\{(env|file):([^}]+)\}")


class ConfigError(Exception):
    """Raised for anything the operator can fix by editing a file or a flag (exit 1)."""


class _NullRedactor:
    """Accepts resolved values and forgets them; used when no redactor was supplied."""

    def add(self, value):
        """Ignore ``value``."""


def interpolate(value, redactor=None, environ=None, source="<config>"):
    """
    Resolve ``${env:NAME}`` and ``${file:PATH}`` placeholders throughout a structure.

    A missing variable or file is a hard error naming both the placeholder and the file
    it came from.  Substituting an empty string instead would produce a run that fails
    three hours later with an authentication error nobody can trace back to here.

    Every resolved value is handed to ``redactor`` so that it is masked in everything the
    CLI prints afterwards, ``--dry-run`` included.
    """
    environ = os.environ if environ is None else environ
    redactor = _NullRedactor() if redactor is None else redactor

    if isinstance(value, dict):
        return {k: interpolate(v, redactor, environ, source) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate(v, redactor, environ, source) for v in value]
    if not isinstance(value, str):
        return value

    resolved = value
    for match in _PLACEHOLDER.finditer(value):
        kind, ref = match.group(1), match.group(2).strip()
        if kind == "env":
            if ref not in environ:
                raise ConfigError(
                    "%s: environment variable %r referenced by ${env:%s} is not set"
                    % (source, ref, ref))
            replacement = environ[ref]
        else:
            path = Path(os.path.expanduser(ref))
            if not path.is_file():
                raise ConfigError(
                    "%s: file %s referenced by ${file:%s} does not exist" % (source, path, ref))
            replacement = path.read_text(encoding="utf-8").strip()
        redactor.add(replacement)
        resolved = resolved.replace(match.group(0), replacement)
    return resolved


def deep_merge(base, overlay):
    """
    Merge ``overlay`` into a copy of ``base``.

    Dicts merge recursively; every other type, lists included, replaces outright.
    Lists concatenating across profiles would make it impossible to *shrink* a list in a
    later layer, which is exactly what an override is for.
    """
    if not isinstance(base, dict) or not isinstance(overlay, dict):
        return copy.deepcopy(overlay)
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def parse_document(text, source="<string>"):
    """
    Parse YAML or JSON, choosing by content rather than by file extension.

    :param text: document body.
    :param source: name used in error messages.
    """
    stripped = text.lstrip()
    try:
        if stripped.startswith(("{", "[")):
            return json.loads(text)
        return yaml.safe_load(text)
    except (ValueError, yaml.YAMLError) as ex:
        raise ConfigError("%s: not valid YAML or JSON: %s" % (source, ex)) from ex


def load_document(path):
    """:return: the parsed contents of ``path``."""
    path = Path(path).expanduser()
    if not path.is_file():
        raise ConfigError("config file not found: %s" % path)
    data = parse_document(path.read_text(encoding="utf-8"), str(path))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError("%s: top level must be a mapping, found %s" % (path, type(data).__name__))
    return data


def resolve_profile(name, profiles_dir):
    """
    Locate a profile by name.

    Search order: ``<profiles_dir>/<name>.yaml`` (then ``.yml``/``.json``), a
    coordinator-relative path, and finally the profiles shipped in ``examples/``.
    """
    candidates = []
    for base in (Path(profiles_dir).expanduser(), Path.cwd()):
        for suffix in (".yaml", ".yml", ".json", ""):
            candidates.append(base / (name + suffix))
    for suffix in (".yaml", ".yml", ".json"):
        candidates.append(PACKAGE_EXAMPLES / ("profile-%s%s" % (name, suffix)))
        candidates.append(PACKAGE_EXAMPLES / (name + suffix))
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise ConfigError("profile %r not found; looked in %s" % (
        name, ", ".join(sorted({str(c.parent) for c in candidates}))))


def validate(config, reference=None, path=""):
    """
    Reject unknown keys with a "did you mean" hint.

    A typo silently ignored in a config that drives a three hour run is expensive; this
    turns it into an immediate, named failure.
    """
    reference = DEFAULTS if reference is None else reference
    for key, value in config.items():
        where = "%s.%s" % (path, key) if path else key
        if key not in reference:
            hint = difflib.get_close_matches(key, list(reference), n=1)
            suffix = "; did you mean %r?" % hint[0] if hint else ""
            raise ConfigError("unknown config key %r%s" % (where, suffix))
        if where in _FREE_FORM_SECTIONS:
            continue
        if isinstance(value, dict) and isinstance(reference[key], dict) and reference[key]:
            validate(value, reference[key], where)


def env_overrides(environ=None):
    """
    Build a config overlay from ``DTR_*`` environment variables.

    ``DTR_CLUSTER__RUNNER=build-vm-01`` sets ``cluster.runner``.  A double underscore
    separates path segments so single underscores stay usable inside key names
    (``DTR_RUN__MAX_PAYLOAD_MB``).  A handful of single-segment aliases cover the flags
    operators reach for most.

    Any other ``DTR_*`` variable is left alone.  Profiles interpolate secrets with
    ``${env:...}``, and those variables are frequently named ``DTR_SOMETHING`` too;
    treating every one of them as a config path would turn a password into a config
    error.  The ``__`` form is still validated, so a typo there is caught.
    """
    environ = os.environ if environ is None else environ
    aliases = {"DTR_RUNNER": "cluster.runner", "DTR_JOBS": "jobs",
               "DTR_CLUSTER": "cluster.name", "DTR_STATE_ROOT": "cluster.state_root",
               "DTR_INSTALL_ROOT": "cluster.install_root", "DTR_USER": "cluster.user"}
    overlay = {}
    for name, raw in sorted(environ.items()):
        if not name.startswith("DTR_") or name in ("DTR_CONFIG", "DTR_PROFILE"):
            continue
        if name in aliases:
            dotted = aliases[name]
        elif "__" in name[len("DTR_"):]:
            dotted = name[len("DTR_"):].lower().replace("__", ".")
        else:
            continue
        set_dotted(overlay, dotted, coerce_scalar(raw))
    return overlay


def coerce_scalar(raw):
    """:return: ``raw`` parsed as JSON when possible, otherwise the string itself."""
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


def set_dotted(target, dotted, value):
    """Set ``a.b.c`` inside a nested dict, creating intermediate dicts as needed."""
    parts = dotted.split(".")
    node = target
    for part in parts[:-1]:
        node = node.setdefault(part, {})
        if not isinstance(node, dict):
            raise ConfigError("cannot set %r: %r is not a mapping" % (dotted, part))
    node[parts[-1]] = value
    return target


def get_dotted(source, dotted, default=None):
    """:return: the value at ``a.b.c`` inside a nested dict, or ``default``."""
    node = source
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def load_config(config_files=(), profiles=(), overrides=None, environ=None,
                user_config=USER_CONFIG_FILE, redactor=None):
    """
    Compose the effective configuration from every layer.

    :param config_files: ``--config`` paths, applied in order.
    :param profiles: ``--profile`` names, applied in order.
    :param overrides: overlay built from explicit command-line flags.
    :param environ: environment mapping, defaults to ``os.environ``.
    :param user_config: path to the per-user config file.
    :param redactor: collects values resolved from ``${env:}`` / ``${file:}``.
    :return: the merged, validated configuration.
    """
    config = copy.deepcopy(DEFAULTS)
    sources = []

    user_config = Path(user_config).expanduser() if user_config else None
    if user_config and user_config.is_file():
        config = _layer(config, load_document(user_config), str(user_config))
        sources.append(str(user_config))

    for path in config_files:
        config = _layer(config, load_document(path), str(path))
        sources.append(str(path))

    for name in profiles:
        path = resolve_profile(name, config.get("profiles_dir", DEFAULTS["profiles_dir"]))
        config = _layer(config, load_document(path), str(path))
        sources.append(str(path))

    config = _layer(config, env_overrides(environ), "environment")

    if overrides:
        config = _layer(config, overrides, "command line")

    config = _interpolate_config(config, sources, redactor, environ)

    config["cluster"]["user"] = config["cluster"]["user"] or _current_user()
    config["_sources"] = sources
    return config


def _interpolate_config(config, sources, redactor, environ):
    """
    Resolve placeholders in every section except the free-form ones.

    ``globals`` and ``parameters`` are left alone here: :mod:`globals_builder` resolves
    them per layer, so its errors can name the profile a placeholder came from, and doing
    it twice would re-scan an already resolved secret for placeholders of its own.
    """
    source = "config (%s)" % (", ".join(sources) if sources else "built-in defaults")
    resolved = {}
    for key, value in config.items():
        if key in _FREE_FORM_SECTIONS:
            resolved[key] = value
        else:
            resolved[key] = interpolate(value, redactor, environ, source)
    return resolved


def _layer(config, overlay, source):
    if not overlay:
        return config
    try:
        validate(overlay)
    except ConfigError as ex:
        raise ConfigError("%s: %s" % (source, ex)) from ex
    return deep_merge(config, overlay)


def _current_user():
    try:
        return getpass.getuser()
    except (KeyError, OSError):  # no passwd entry, e.g. some containers
        return os.environ.get("USER") or os.environ.get("USERNAME") or "root"


def expand_path(path, default=None):
    """:return: ``path`` with ``~`` expanded, or ``default`` when path is falsy."""
    if not path:
        return default
    return os.path.expanduser(str(path))


def redacted_summary(config):
    """:return: a config copy safe to write into ``meta.json`` (globals stripped)."""
    summary = copy.deepcopy({k: v for k, v in config.items() if not k.startswith("_")})
    summary["globals"] = "<redacted: see globals.json, mode 0600>"
    summary["parameters"] = "<redacted>"
    return summary
