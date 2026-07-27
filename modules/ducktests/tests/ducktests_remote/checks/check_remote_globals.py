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

"""Checks for globals composition, interpolation and secret redaction."""

import json

import pytest

from ducktests_remote.config import ConfigError
from ducktests_remote.globals_builder import (Redactor, build, dumps, load_raw_layer,
                                              parse_kv_override)

SECRET = "s3cr3t-passphrase"


class CheckLayering:
    """Deep merge, list replacement, override precedence."""

    def check_later_layer_wins_per_key(self):
        composed, _ = build([("base", {"ssl": {"enabled": False}, "project": "ignite"}),
                             ("profile", {"ssl": {"enabled": True}})])
        assert composed == {"ssl": {"enabled": True}, "project": "ignite"}

    def check_lists_replace(self):
        composed, _ = build([("base", {"ignite_versions": ["2.16.0", "2.17.0"]}),
                             ("profile", {"ignite_versions": ["dev"]})])
        assert composed["ignite_versions"] == ["dev"]

    def check_raw_jenkins_blob_is_a_usable_base_layer(self):
        blob = '{"project": "ise", "ignite_versions": ["ise-0-32"]}'
        raw = load_raw_layer(blob, None)
        composed, _ = build([("blob", raw), ("profile", {"project": "ignite"})])
        assert composed == {"project": "ignite", "ignite_versions": ["ise-0-32"]}

    def check_raw_blob_must_be_an_object(self):
        with pytest.raises(ConfigError):
            load_raw_layer("[1, 2, 3]", None)


class CheckOverrides:
    """``-g a.b.c=value`` mechanics."""

    def check_dotted_path_nests(self):
        composed, _ = build([], ["ssl.key_store.path=/tmp/ks"])
        assert composed == {"ssl": {"key_store": {"path": "/tmp/ks"}}}

    def check_json_values_are_coerced(self):
        composed, _ = build([], ["ssl.enabled=true", "count=3", "project=ise"])
        assert composed["ssl"]["enabled"] is True
        assert composed["count"] == 3
        assert composed["project"] == "ise", "a bare word stays a string"

    def check_override_beats_the_layers(self):
        composed, _ = build([("profile", {"project": "ignite"})], ["project=ise"])
        assert composed["project"] == "ise"

    def check_missing_equals_is_an_error(self):
        with pytest.raises(ConfigError):
            parse_kv_override("just-a-key")


class CheckInterpolation:
    """``${env:}`` and ``${file:}``."""

    def check_env_is_resolved(self):
        composed, _ = build([("p", {"authentication": {"password": "${env:ISE_PASSWORD}"}})],
                            environ={"ISE_PASSWORD": SECRET})
        assert composed["authentication"]["password"] == SECRET

    def check_missing_env_names_the_variable_and_the_file(self):
        with pytest.raises(ConfigError) as ex:
            build([("profile-ise.yaml", {"p": "${env:NOT_SET_ANYWHERE}"})], environ={})
        message = str(ex.value)
        assert "NOT_SET_ANYWHERE" in message and "profile-ise.yaml" in message

    def check_missing_env_never_becomes_an_empty_string(self):
        with pytest.raises(ConfigError):
            build([("p", {"password": "${env:ABSENT}"})], environ={})

    def check_file_is_read_and_trimmed(self, tmp_path):
        path = tmp_path / "pass.txt"
        path.write_text(SECRET + "\n", encoding="utf-8")
        composed, _ = build([("p", {"password": "${file:%s}" % path})], environ={})
        assert composed["password"] == SECRET

    def check_missing_file_is_an_error(self, tmp_path):
        with pytest.raises(ConfigError):
            build([("p", {"password": "${file:%s}" % (tmp_path / "nope")})], environ={})

    def check_placeholders_inside_lists_are_resolved(self):
        composed, _ = build([("p", {"versions": ["${env:V}"]})], environ={"V": "ise-0-32"})
        assert composed["versions"] == ["ise-0-32"]


class CheckRedaction:
    """A resolved secret must not survive anywhere the CLI writes."""

    def _composed(self):
        redactor = Redactor()
        composed, _ = build([("p", {"authentication": {"password": "${env:ISE_PASSWORD}"},
                                    "note": "connect with %s please" % SECRET})],
                            redactor=redactor, environ={"ISE_PASSWORD": SECRET})
        return composed, redactor

    def check_value_based_redaction_catches_an_unrelated_field(self):
        composed, redactor = self._composed()
        masked = redactor.redact_structure(composed)
        assert SECRET not in json.dumps(masked)
        assert masked["note"] == "connect with *** please"

    def check_rendered_output_is_clean(self):
        composed, redactor = self._composed()
        rendered = dumps(redactor.redact_structure(composed))
        assert SECRET not in rendered

    def check_arbitrary_text_is_redacted(self):
        _, redactor = self._composed()
        line = "ssh failed: tried password %s" % SECRET
        assert SECRET not in redactor.redact(line)

    def check_key_name_fallback_masks_unresolved_secrets(self):
        redactor = Redactor()
        masked = redactor.redact_structure({"password": "typed-inline-not-from-env"})
        assert masked["password"] == "***"

    def check_the_written_globals_file_still_contains_the_real_value(self):
        # Redaction is for output only; ducktape needs the real credentials on the runner,
        # which is why globals.json is written with mode 0600 and excluded from fetch.
        composed, _ = self._composed()
        assert composed["authentication"]["password"] == SECRET
