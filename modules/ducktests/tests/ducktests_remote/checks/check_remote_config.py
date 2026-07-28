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

"""Checks for configuration discovery, layering and validation."""

import pytest

from ducktests_remote.config import (ConfigError, coerce_scalar, deep_merge, env_overrides,
                                     get_dotted, load_config, parse_document, set_dotted,
                                     validate)


class CheckMerge:
    """Layering semantics."""

    def check_dicts_merge_recursively(self):
        base = {"a": {"x": 1, "y": 2}, "b": 1}
        overlay = {"a": {"y": 3, "z": 4}}
        assert deep_merge(base, overlay) == {"a": {"x": 1, "y": 3, "z": 4}, "b": 1}

    def check_lists_replace_and_do_not_concatenate(self):
        merged = deep_merge({"v": ["a", "b", "c"]}, {"v": ["d"]})
        assert merged["v"] == ["d"], "a later layer must be able to shrink a list"

    def check_base_is_not_mutated(self):
        base = {"a": {"x": 1}}
        deep_merge(base, {"a": {"x": 2}})
        assert base == {"a": {"x": 1}}


class CheckDocuments:
    """Parser selection by content, not by extension."""

    def check_json_body(self):
        assert parse_document('{"cluster": {"name": "lab"}}') == {"cluster": {"name": "lab"}}

    def check_yaml_body(self):
        assert parse_document("cluster:\n  name: lab\n") == {"cluster": {"name": "lab"}}

    def check_broken_body_names_the_source(self):
        with pytest.raises(ConfigError) as ex:
            parse_document("cluster: [unclosed", source="profile.yaml")
        assert "profile.yaml" in str(ex.value)


class CheckValidation:
    """Unknown keys are a hard error with a suggestion."""

    def check_unknown_top_level_key_is_rejected(self):
        with pytest.raises(ConfigError) as ex:
            validate({"clustr": {}})
        assert "clustr" in str(ex.value) and "cluster" in str(ex.value)

    def check_unknown_nested_key_is_rejected(self):
        with pytest.raises(ConfigError) as ex:
            validate({"cluster": {"instal_root": "/opt"}})
        assert "cluster.instal_root" in str(ex.value)

    def check_globals_are_free_form(self):
        validate({"globals": {"anything_at_all": {"nested": 1}}})

    def check_parameters_are_free_form(self):
        validate({"parameters": {"whatever": 1}})


class CheckDotted:
    """Dotted path helpers."""

    def check_set_creates_intermediate_dicts(self):
        target = {}
        set_dotted(target, "a.b.c", 1)
        assert target == {"a": {"b": {"c": 1}}}

    def check_get_returns_default_for_missing(self):
        assert get_dotted({"a": {"b": 1}}, "a.z", "fallback") == "fallback"

    def check_scalar_coercion(self):
        assert coerce_scalar("true") is True
        assert coerce_scalar("12") == 12
        assert coerce_scalar("ise") == "ise"


class CheckEnvironment:
    """DTR_* overrides."""

    def check_double_underscore_is_a_path_separator(self):
        overlay = env_overrides({"DTR_CLUSTER__RUNNER": "build-vm-01"})
        assert overlay == {"cluster": {"runner": "build-vm-01"}}

    def check_single_underscores_survive_inside_a_key(self):
        overlay = env_overrides({"DTR_RUN__MAX_PAYLOAD_MB": "50"})
        assert overlay == {"run": {"max_payload_mb": 50}}

    def check_aliases(self):
        assert env_overrides({"DTR_RUNNER": "vm"}) == {"cluster": {"runner": "vm"}}

    def check_secret_variables_are_not_treated_as_config(self):
        # Profiles interpolate ${env:DTR_...}; those must not become config paths.
        assert env_overrides({"DTR_ISE_PASSWORD": "hunter2"}) == {}


class CheckLoad:
    """Whole-stack layering."""

    def check_later_config_file_wins(self, tmp_path):
        first = tmp_path / "a.yaml"
        first.write_text("cluster:\n  name: one\n  port: 2222\n", encoding="utf-8")
        second = tmp_path / "b.yaml"
        second.write_text("cluster:\n  name: two\n", encoding="utf-8")
        config = load_config(config_files=[first, second], environ={}, user_config=None)
        assert config["cluster"]["name"] == "two"
        assert config["cluster"]["port"] == 2222, "unrelated keys survive the overlay"

    def check_flags_beat_environment(self, tmp_path):
        config = load_config(environ={"DTR_RUNNER": "from-env"},
                             overrides={"cluster": {"runner": "from-flag"}},
                             user_config=None)
        assert config["cluster"]["runner"] == "from-flag"

    def check_user_defaults_to_the_coordinator_account_not_ducker(self):
        config = load_config(environ={}, user_config=None)
        assert config["cluster"]["user"], "an ssh user must always be resolved"
        assert config["cluster"]["user"] != "ducker", \
            "nothing may default to the Docker image's account"

    def check_missing_config_file_is_reported(self):
        with pytest.raises(ConfigError) as ex:
            load_config(config_files=["/nonexistent/nope.yaml"], environ={}, user_config=None)
        assert "not found" in str(ex.value)


class CheckRenamedKeys:
    """Keys that moved out of `provision` when pip and java got sections of their own."""

    def check_the_old_pip_index_key_is_rejected_with_a_hint(self):
        with pytest.raises(ConfigError) as ex:
            validate({"provision": {"pip_index_url": "https://nexus.invalid/simple"}})
        assert "provision.pip_index_url" in str(ex.value)

    def check_the_old_jdk_major_key_is_rejected(self):
        with pytest.raises(ConfigError) as ex:
            validate({"provision": {"jdk_major": 17}})
        assert "provision.jdk_major" in str(ex.value)

    def check_the_new_sections_validate(self):
        validate({"pip": {"index_url": "https://nexus.invalid/simple", "timeout": 60},
                  "java": {"major": 17, "home": "/opt/jdk-17", "bashrc": False}})

    def check_a_typo_in_the_new_sections_is_caught(self):
        with pytest.raises(ConfigError) as ex:
            validate({"java": {"majr": 17}})
        assert "java.majr" in str(ex.value) and "major" in str(ex.value)


class CheckInterpolation:
    """``${env:}`` / ``${file:}`` outside the globals section."""

    def check_a_placeholder_in_a_config_section_is_resolved(self):
        # examples/cluster.yaml has always advertised this; before pip.* needed it, the
        # placeholder was passed through literally and ssh went looking for a host named
        # "${env:DTR_RUNNER}".
        config = load_config(environ={"DTR_TEST_RUNNER": "build-vm-01"}, user_config=None,
                             overrides={"cluster": {"runner": "${env:DTR_TEST_RUNNER}"}})
        assert config["cluster"]["runner"] == "build-vm-01"

    def check_a_missing_variable_names_the_variable(self):
        with pytest.raises(ConfigError) as ex:
            load_config(environ={}, user_config=None,
                        overrides={"pip": {"index_url": "${env:DTR_ABSENT_INDEX}"}})
        assert "DTR_ABSENT_INDEX" in str(ex.value)

    def check_a_resolved_value_is_registered_for_redaction(self):
        from ducktests_remote.globals_builder import Redactor  # noqa: PLC0415

        redactor = Redactor()
        load_config(environ={"DTR_TEST_INDEX": "https://bob:s3cret@nexus.invalid/simple"},
                    user_config=None, redactor=redactor,
                    overrides={"pip": {"index_url": "${env:DTR_TEST_INDEX}"}})
        assert redactor.redact("index is https://bob:s3cret@nexus.invalid/simple") \
            == "index is ***"

    def check_globals_are_left_for_the_globals_builder(self):
        # Resolved there, per layer, so the error can name the profile it came from.
        config = load_config(environ={}, user_config=None,
                             overrides={"globals": {"password": "${env:DTR_ABSENT}"}})
        assert config["globals"]["password"] == "${env:DTR_ABSENT}"
