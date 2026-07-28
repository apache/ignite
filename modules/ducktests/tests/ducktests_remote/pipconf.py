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
Index configuration for the pip commands the CLI runs on the runner.

A runner inside a corporate network usually cannot reach PyPI at all, only an internal
mirror, and frequently one behind a private CA.  Everything needed for that is expressed
in the ``pip`` config section and turned into *command-line flags* here.

Flags rather than a generated ``pip.conf`` or ``PIP_*`` environment variables: ``--dry-run``
printing the literal command it would run is this CLI's contract, and configuration hidden
in a file the operator cannot see breaks it.  pip carries its finder settings into PEP 517
build isolation, so ``pip install -e`` picks the same index up for its build dependencies.

The module is named ``pipconf`` rather than ``pip`` so that it can never shadow the real
``pip`` package for anything that ends up with this directory on ``sys.path``.
"""

import os
import re
import shlex

from ducktests_remote.config import ConfigError

# Matches the credentials in https://user:token@host/simple, which is how an internal
# index is usually handed out.
_USERINFO = re.compile(r"(?<=://)[^/@\s]+(?=@)")


def pip_args(config):
    """
    :return: the pip flags implied by the ``pip`` config section, as an argv list.

    Empty configuration yields an empty list, so a command line with nothing configured
    stays byte for byte what it was before this section existed.
    """
    section = (config or {}).get("pip") or {}
    args = []

    index_url = _text(section.get("index_url"), "pip.index_url")
    if index_url:
        args += ["--index-url", index_url]
    for url in _as_list(section.get("extra_index_url"), "pip.extra_index_url"):
        args += ["--extra-index-url", url]
    for host in _as_list(section.get("trusted_host"), "pip.trusted_host"):
        args += ["--trusted-host", host]

    timeout = _positive_int(section.get("timeout"), "pip.timeout")
    if timeout is not None:
        args += ["--timeout", str(timeout)]
    retries = _positive_int(section.get("retries"), "pip.retries")
    if retries is not None:
        args += ["--retries", str(retries)]

    cert = _text(section.get("cert"), "pip.cert")
    if cert:
        # A runner-side path, like cluster.identity_file: it is opened by pip on the
        # runner, so a file that exists on this machine proves nothing.
        args += ["--cert", os.path.expanduser(cert) if cert.startswith("~") else cert]

    return args


def pip_args_str(config):
    """:return: :func:`pip_args` shell-quoted for splicing into a generated script."""
    return " ".join(shlex.quote(arg) for arg in pip_args(config))


def describe(config, redactor=None):
    """
    :return: a one-line, credential-free summary of where pip will install from.

    The value-keyed :class:`~ducktests_remote.globals_builder.Redactor` only knows about
    values it resolved itself, so an index URL typed straight into a config file would
    otherwise reach the terminal with its token attached.  Masking the userinfo here
    covers that case regardless of where the URL came from.
    """
    section = (config or {}).get("pip") or {}
    parts = []
    index_url = _text(section.get("index_url"), "pip.index_url")
    parts.append("index %s" % mask_credentials(index_url) if index_url
                 else "index default (PyPI)")
    extras = _as_list(section.get("extra_index_url"), "pip.extra_index_url")
    if extras:
        parts.append("extra %s" % ", ".join(mask_credentials(url) for url in extras))
    hosts = _as_list(section.get("trusted_host"), "pip.trusted_host")
    if hosts:
        parts.append("trusted %s" % ", ".join(hosts))
    if section.get("cert"):
        parts.append("cert %s" % section["cert"])
    timeout = _positive_int(section.get("timeout"), "pip.timeout")
    if timeout is not None:
        parts.append("timeout %ds" % timeout)
    retries = _positive_int(section.get("retries"), "pip.retries")
    if retries is not None:
        parts.append("retries %d" % retries)
    line = ", ".join(parts)
    return redactor.redact(line) if redactor is not None else line


def mask_credentials(url):
    """:return: ``url`` with any ``user:password@`` replaced by ``***@``."""
    return _USERINFO.sub("***", str(url or ""))


def _text(value, key):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError("%s must be a string, found %s" % (key, type(value).__name__))
    return value.strip() or None


def _as_list(value, key):
    """A single string is accepted wherever a list is: that is what operators type."""
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple)):
        values = list(value)
    else:
        raise ConfigError("%s must be a string or a list, found %s"
                          % (key, type(value).__name__))
    out = []
    for item in values:
        if not isinstance(item, str):
            raise ConfigError("%s: every entry must be a string, found %s"
                              % (key, type(item).__name__))
        if item.strip():
            out.append(item.strip())
    return out


def _positive_int(value, key):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ConfigError("%s must be a whole number of seconds, found %r" % (key, value))
    try:
        number = int(value)
    except ValueError as ex:
        raise ConfigError("%s must be a whole number, found %r" % (key, value)) from ex
    if number <= 0:
        raise ConfigError("%s must be greater than zero, found %d" % (key, number))
    return number
