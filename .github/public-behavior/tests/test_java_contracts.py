# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest

from contract_diff import java_changes


def compare(before, after):
    """Run the workflow's JDK parser on both snapshots."""
    sources = [((side, path), source) for side, files in (("before", before), ("after", after))
               for path, source in files.items()]
    return ["\t".join(row) for row in java_changes(sources)]


class JavaContractsTest(unittest.TestCase):
    def test_reports_api_declarations_and_literal_values(self):
        before = {"api/Client.java": """package api;
public interface Client { int TIMEOUT = 3; int read(String key); void remove(); }"""}
        after = {"api/Client.java": """package api;
public interface Client { int TIMEOUT = 5; long read(String key); void put(int key); }"""}
        self.assertEqual(compare(before, after), [
            "api/Client.java\tChanged API constant api.Client.TIMEOUT from 3 to 5",
            "api/Client.java\tAdded API method api.Client.put (1 parameter)",
            "api/Client.java\tChanged API method api.Client.read (1 parameter) from public (String) -> int to public (String) -> long",
            "api/Client.java\tRemoved API method api.Client.remove (0 parameters)",
        ])

    def test_preserves_contract_across_source_only_edits(self):
        before = {"old/Client.java": """package api;
import java.util.List;
public class Client {
    public List<String> list(String key) { throw new UnsupportedOperationException(); }
    private class Helper { public void foo() {} }
}"""}
        after = {"new/Client.java": """package api;
public class Client {
    private class Helper { public void bar() {} }
    @Override public String toString() { return "client"; }
    public java.util.List<String> list(String name) {
        class Local { public int unrelated() { return 1; } }
        return new java.util.ArrayList<>();
    }
}"""}
        self.assertEqual(compare(before, after), [])

    def test_ignores_inherited_methods_without_type_information(self):
        before = {"api/Child.java": "package api; public class Child extends Parent {}"}
        after = {"api/Child.java": "package api; public class Child extends Parent { public void inherited() {} }"}
        self.assertEqual(compare(before, after), [])

    def test_equal_literal_values_after_field_conversion(self):
        before = {"Settings.java": 'public class Settings { public static final double RATE = 1;'
                  " public static final int CODE = 'A'; }"}
        after = {"Settings.java": "public class Settings { public static final double RATE = 1.0;"
                 " public static final int CODE = 65; }"}
        self.assertEqual(compare(before, after), [])

    def test_members_can_move_to_an_unknown_parent(self):
        own = {"Settings.java": "public class Settings { public int timeout() { return 1; }"
               " public static final int DEFAULT = 1; }"}
        inherited = {"Settings.java": "public class Settings extends Base {}"}
        self.assertEqual(compare(own, inherited), [])
        self.assertEqual(compare(inherited, own), [])

    def test_removed_deprecated_api_is_left_to_review(self):
        before = {"Api.java": """public interface Api {
            @java.lang.Deprecated void oldMethod();
            @Deprecated int OLD_SETTING = 5;
            @Deprecated class Legacy { public void read() {} }
        }"""}
        after = {"Api.java": "public interface Api { void newMethod(); }"}
        self.assertEqual(compare(before, after), ["Api.java\tAdded API method Api.newMethod (0 parameters)"])

    def test_protocol_values_track_wire_ids_across_java_renames(self):
        pkg = "package org.apache.ignite.internal.client.thin;"
        before = {"ClientOperation.java": pkg + " enum ClientOperation { CACHE(1), READ(-1); ClientOperation(int n) {} }"}
        after = {"ClientOperation.java": pkg + " enum ClientOperation { RENAMED(1), READ(3); ClientOperation(int n) {} }"}
        self.assertEqual(compare(before, after), [
            "ClientOperation.java\tChanged client operation ID for READ from -1 to 3"])

    def test_swapping_existing_wire_ids_reports_both_operations(self):
        pkg = "package org.apache.ignite.internal.client.thin;"
        before = {"ClientOperation.java": pkg + " enum ClientOperation { A(1), B(2); ClientOperation(int n) {} }"}
        after = {"ClientOperation.java": pkg + " enum ClientOperation { B(1), A(2); ClientOperation(int n) {} }"}
        self.assertEqual(compare(before, after), [
            "ClientOperation.java\tChanged client operation ID for A from 1 to 2",
            "ClientOperation.java\tChanged client operation ID for B from 2 to 1"])

    def test_added_wire_file_lists_its_known_values(self):
        after = {"ClientBitmaskFeature.java": "package org.apache.ignite.internal.processors.platform.client;"
                 " enum ClientBitmaskFeature { A(1); ClientBitmaskFeature(int n) {} }"}
        self.assertEqual(compare({}, after), ["ClientBitmaskFeature.java\tAdded client feature ID 1"])
        self.assertEqual(compare(after, {}), ["ClientBitmaskFeature.java\tRemoved client feature ID 1"])

    def test_sql_states_are_values_even_with_duplicate_names(self):
        pkg = "package org.apache.ignite.internal.processors.odbc;"
        before = {"SqlStateCode.java": pkg + ' class SqlStateCode { public static final String E="02000", F="01000"; }'}
        after = {"SqlStateCode.java": pkg + ' class SqlStateCode { public static final String A="02000", B="02000", F="22000"; }'}
        self.assertEqual(compare(before, after), [
            "SqlStateCode.java\tChanged SQLState for F from 01000 to 22000"])

    def test_sql_error_codes_use_the_declared_integer_value(self):
        pkg = "package org.apache.ignite.internal.processors.cache.query;"
        before = {"IgniteQueryErrorCode.java": pkg + " class IgniteQueryErrorCode { public static final int E='A'; }"}
        after = {"IgniteQueryErrorCode.java": pkg + " class IgniteQueryErrorCode { public static final int E=65;"
                 ' public static final String DESCRIPTION="example"; }'}
        self.assertEqual(compare(before, after), [])

    def test_parse_error_does_not_report_a_removal_or_hide_other_files(self):
        src = "package api; public class Valid { public void newSetting() {} }"
        before = {"api/Bad.java": "package api; public class Bad { public int run() { return 1; } }"}
        after = {"api/Bad.java": "package api; public class Bad { public int }", "api/Valid.java": src}
        rows = compare(before, after)
        self.assertTrue(any("Added API method api.Valid.newSetting" in row for row in rows), rows)
        self.assertFalse(any("Bad" in row for row in rows), rows)

    def test_constructors_of_derived_classes_and_public_enum_members(self):
        """A new constructor needs no inherited-method resolution. Enum members keep their source names."""
        before = {"Client.java": "public class Client extends Base { public Client(String s) {} }",
                  "Mode.java": "public enum Mode implements Marker { FIRST, SECOND }"}
        after = {"Client.java": before["Client.java"].replace("String s", "long value"),
                 "Mode.java": before["Mode.java"].replace("SECOND", "THIRD")}
        self.assertCountEqual(compare(before, after), [
            "Client.java\tChanged API constructor Client (1 parameter) from public (String) to public (long)",
            "Mode.java\tRemoved API enum value Mode.SECOND", "Mode.java\tAdded API enum value Mode.THIRD"])
        self.assertEqual(compare(before, {**before, "Mode.java": before["Mode.java"].replace("FIRST, SECOND", "SECOND, FIRST")}), [])
