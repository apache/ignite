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

"""Checks for the pip index configuration and for the commands that carry it."""

import pytest

from fake_transport import FakeTransport

from ducktests_remote import pipconf
from ducktests_remote.cli import Console, Context
from ducktests_remote.commands import run
from ducktests_remote.config import ConfigError, load_config


def _config(**pip):
    config = load_config(user_config=None)
    config["pip"].update(pip)
    return config


class CheckArguments:
    """What ends up on the pip command line."""

    def check_nothing_configured_adds_no_arguments(self):
        # The rendered command has to stay byte for byte what it was before this section
        # existed, or every golden-string check in here becomes a lie.
        assert pipconf.pip_args(_config()) == []
        assert pipconf.pip_args_str(_config()) == ""

    def check_full_configuration_in_a_fixed_order(self):
        config = _config(index_url="https://nexus.invalid/simple",
                         extra_index_url=["https://nexus.invalid/internal"],
                         trusted_host=["nexus.invalid"], timeout=60, retries=5,
                         cert="/etc/pki/corp.pem")
        assert pipconf.pip_args(config) == [
            "--index-url", "https://nexus.invalid/simple",
            "--extra-index-url", "https://nexus.invalid/internal",
            "--trusted-host", "nexus.invalid",
            "--timeout", "60",
            "--retries", "5",
            "--cert", "/etc/pki/corp.pem",
        ]

    def check_a_bare_string_is_accepted_where_a_list_is_expected(self):
        config = _config(extra_index_url="https://a.invalid/s", trusted_host="a.invalid")
        assert pipconf.pip_args(config) == ["--extra-index-url", "https://a.invalid/s",
                                            "--trusted-host", "a.invalid"]

    def check_repeatable_values_are_repeated(self):
        config = _config(trusted_host=["a.invalid", "b.invalid"])
        assert pipconf.pip_args(config).count("--trusted-host") == 2

    def check_blank_entries_are_dropped(self):
        assert pipconf.pip_args(_config(index_url="   ", trusted_host=["", " "])) == []

    def check_arguments_are_quoted_for_a_script(self):
        rendered = pipconf.pip_args_str(_config(cert="/etc/pki/corp ca.pem"))
        assert "'/etc/pki/corp ca.pem'" in rendered

    def check_a_bad_timeout_names_the_key(self):
        with pytest.raises(ConfigError) as ex:
            pipconf.pip_args(_config(timeout="soon"))
        assert "pip.timeout" in str(ex.value)

    def check_a_negative_timeout_is_rejected(self):
        with pytest.raises(ConfigError):
            pipconf.pip_args(_config(timeout=0))

    def check_a_non_string_index_names_the_key(self):
        with pytest.raises(ConfigError) as ex:
            pipconf.pip_args(_config(index_url=["https://a.invalid"]))
        assert "pip.index_url" in str(ex.value)


class CheckDescription:
    """The one-line summary doctor prints."""

    def check_credentials_never_reach_the_terminal(self):
        # This URL came straight from a config file, so the value-keyed redactor has
        # never seen it; masking the userinfo is the only thing standing in the way.
        line = pipconf.describe(_config(index_url="https://bob:s3cret@nexus.invalid/simple"))
        assert "s3cret" not in line and "bob" not in line
        assert "https://***@nexus.invalid/simple" in line

    def check_no_configuration_says_pypi(self):
        assert "PyPI" in pipconf.describe(_config())

    def check_every_configured_value_is_mentioned(self):
        line = pipconf.describe(_config(index_url="https://nexus.invalid/simple",
                                        trusted_host="nexus.invalid", timeout=60,
                                        retries=5, cert="/etc/pki/corp.pem"))
        for expected in ("nexus.invalid", "trusted", "60", "5", "corp.pem"):
            assert expected in line


def _context(config, **args):
    class Args:  # pylint: disable=too-few-public-methods
        """Minimal stand-in for the parsed command line."""

    parsed = Args()
    parsed.dry_run = False
    for key, value in args.items():
        setattr(parsed, key, value)
    ctx = Context(config, parsed, Console(color=False))
    ctx._runner = FakeTransport()  # noqa: SLF001 - the point of the fake
    return ctx


class CheckCallSites:
    """Both pip invocations have to carry the configuration, not just the first."""

    def check_the_venv_install_carries_the_index(self):
        config = _config(index_url="https://nexus.invalid/simple", trusted_host="nexus.invalid")
        ctx = _context(config)
        ctx.runner.when("import ducktape", "ducktape 0.13.0")
        run._ensure_venv(ctx, "/work")  # noqa: SLF001 - internal by design
        script = "\n".join(ctx.runner.scripts)
        assert "--index-url https://nexus.invalid/simple" in script
        assert "--trusted-host nexus.invalid" in script

    def check_install_sources_carries_the_index_too(self):
        # It did not, before pip.* existed: `--install-sources` went to PyPI whatever the
        # configured index was, and the run failed minutes into a job nobody was watching.
        ctx = _context(_config(index_url="https://nexus.invalid/simple"))
        run._install_sources(ctx, "/work")  # noqa: SLF001 - internal by design
        argv = ctx.runner.commands[-1]
        assert "--index-url" in argv and "https://nexus.invalid/simple" in argv
        assert argv[-2:] == ["-e", "/work/modules/ducktests/tests"]

    def check_an_unconfigured_venv_script_stays_unchanged(self):
        ctx = _context(_config())
        ctx.runner.when("import ducktape", "ducktape 0.13.0")
        run._ensure_venv(ctx, "/work")  # noqa: SLF001 - internal by design
        assert "install --disable-pip-version-check  -r" in "\n".join(ctx.runner.scripts)
