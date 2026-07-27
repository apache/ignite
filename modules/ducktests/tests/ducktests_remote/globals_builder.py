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
Composition of the ``--globals`` payload, and the redactor that keeps its secrets out of
everything the CLI prints.

The Jenkins one-liner this replaces carries a kilobyte of JSON with a password and a set
of internal IPs on the command line, where it lands in shell history, process listings
and build logs.  Here the same content is layered from profiles, resolved from the
environment at the last moment, and written straight to a 0600 file on the runner.
"""

import json
import os
import re
from pathlib import Path

from ducktests_remote.config import ConfigError, deep_merge, set_dotted

_PLACEHOLDER = re.compile(r"\$\{(env|file):([^}]+)\}")


class Redactor:
    """
    Replaces resolved secret *values* with ``***`` in anything the CLI emits.

    Keying on values rather than on key names is what makes this reliable: a password
    that leaks into an unrelated field, a rendered command line or a traceback is still
    caught.  Key-name matching is only a fallback for values we never resolved ourselves.
    """

    MASK = "***"
    SENSITIVE_KEYS = ("password", "passwd", "secret", "token", "keystore_pass", "truststore_pass")

    def __init__(self):
        self._values = set()

    def add(self, value):
        """Register a resolved secret value."""
        if isinstance(value, str) and len(value.strip()) >= 3:
            self._values.add(value.strip())

    @property
    def values(self):
        """:return: the registered secret values."""
        return frozenset(self._values)

    def redact(self, text):
        """:return: ``text`` with every registered secret replaced by ``***``."""
        if text is None:
            return None
        result = str(text)
        for secret in sorted(self._values, key=len, reverse=True):
            result = result.replace(secret, self.MASK)
        return result

    def redact_structure(self, data):
        """:return: a copy of ``data`` with secret values, and sensitive keys, masked."""
        if isinstance(data, dict):
            out = {}
            for key, value in data.items():
                if isinstance(key, str) and key.lower() in self.SENSITIVE_KEYS:
                    out[key] = self.MASK
                else:
                    out[key] = self.redact_structure(value)
            return out
        if isinstance(data, list):
            return [self.redact_structure(item) for item in data]
        if isinstance(data, str):
            return self.redact(data)
        return data


def interpolate(value, redactor, environ=None, source="<config>"):
    """
    Resolve ``${env:NAME}`` and ``${file:PATH}`` placeholders throughout a structure.

    A missing variable or file is a hard error naming both the placeholder and the file
    it came from.  Substituting an empty string instead would produce a run that fails
    three hours later with an authentication error nobody can trace back to here.
    """
    environ = os.environ if environ is None else environ

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


def parse_kv_override(item):
    """
    Parse a ``-g a.b.c=value`` argument.

    Values are parsed as JSON when they parse, so ``ssl.enabled=true`` yields a boolean
    and ``project=ise`` yields a string.  This mirrors ``_extend_json`` in
    ``docker/run_tests.sh``, with nesting added.
    """
    if "=" not in item:
        raise ConfigError("expected KEY=VALUE, found %r" % item)
    key, raw = item.split("=", 1)
    key = key.strip()
    if not key:
        raise ConfigError("empty key in %r" % item)
    try:
        value = json.loads(raw)
    except ValueError:
        value = raw
    return key, value


def build(base_layers=(), overrides=(), redactor=None, environ=None):
    """
    Compose the final globals mapping.

    :param base_layers: ``(source_name, mapping)`` pairs, later layers winning.
    :param overrides: raw ``KEY=VALUE`` strings from ``-g``.
    :param redactor: :class:`Redactor` collecting resolved secrets; created when omitted.
    :param environ: environment mapping used for ``${env:}``.
    :return: ``(globals_dict, redactor)``.
    """
    redactor = Redactor() if redactor is None else redactor
    composed = {}
    for source, layer in base_layers:
        if not layer:
            continue
        if not isinstance(layer, dict):
            raise ConfigError("%s: globals must be a mapping, found %s"
                              % (source, type(layer).__name__))
        composed = deep_merge(composed, interpolate(layer, redactor, environ, source))

    for item in overrides:
        key, value = parse_kv_override(item)
        set_dotted(composed, key, interpolate(value, redactor, environ, "-g %s" % key))

    return composed, redactor


def load_raw_layer(json_text=None, json_file=None):
    """
    Parse a raw base layer from ``--globals-json`` / ``--globals-file``.

    This is the migration path: paste the existing Jenkins blob verbatim, get a working
    run, then split it into YAML profiles one key at a time.
    """
    if json_text and json_file:
        raise ConfigError("pass only one of --globals-json / --globals-file")
    if json_file:
        path = Path(os.path.expanduser(json_file))
        if not path.is_file():
            raise ConfigError("globals file not found: %s" % path)
        json_text = path.read_text(encoding="utf-8")
    if not json_text:
        return None
    try:
        data = json.loads(json_text)
    except ValueError as ex:
        raise ConfigError("globals is not valid JSON: %s" % ex) from ex
    if not isinstance(data, dict):
        raise ConfigError("globals must be a JSON object, found %s" % type(data).__name__)
    return data


def dumps(data):
    """:return: canonical JSON for a globals/parameters payload."""
    return json.dumps(data, indent=2, sort_keys=True) + "\n"
