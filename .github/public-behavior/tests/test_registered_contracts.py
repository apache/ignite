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

from pathlib import PurePosixPath

from contract_diff import compare_sources
from registered_contracts import REGISTERED_PATHS


def compare(before, after):
    """Use the checker's real source paths and compare two versions of each registration."""
    paths = {PurePosixPath(path).name: path for path in REGISTERED_PATHS}
    pairs = [(paths[name], paths[name]) for name in before]
    sources = {(side, paths[name]): src for side, snapshot in (("before", before), ("after", after))
               for name, src in snapshot.items()}
    changes = compare_sources(pairs, sources)
    return [f"{PurePosixPath(path).name}\t{finding}" for path, findings in changes.items() for finding in findings]


class RegisteredContractsTest(unittest.TestCase):
    def test_reports_the_default_of_a_registered_setting(self):
        """When the registered setting's default changes, show its user-facing key and both values."""
        source = """package org.apache.ignite.internal.processors.query.h2;
            class DistributedIndexingConfiguration {
                private final DistributedBooleanProperty disable =
                    DistributedBooleanProperty.detachedBooleanProperty("sql.disableCreateLuceneIndexForStringValueType", "description");
                void onReadyToRegister(DistributedPropertyDispatcher dispatcher) { dispatcher.registerProperty(disable); }
                void onReadyToWrite() { setDefaultValue(disable, false, log); }
            }"""
        path = "DistributedIndexingConfiguration.java"
        self.assertEqual(compare({path: source}, {path: source.replace("false", "true")}), [
            path + "\tChanged default for distributed setting sql.disableCreateLuceneIndexForStringValueType from false to true"
        ])

    def test_reports_public_names_from_registrations(self):
        """Renaming the same registered setting, metric, view or SQL function identifies the client's old and new names."""
        before = {
            "DistributedBaselineConfiguration.java": """package org.apache.ignite.internal.cluster;
                class DistributedBaselineConfiguration {
                    private final DistributedChangeableProperty<Long> delay = detachedLongProperty("baselineAutoAdjustTimeout", "help");
                    void onReadyToRegister(DistributedPropertyDispatcher dispatcher) { dispatcher.registerProperties(delay); }
                }""",
            "QueryParserMetricsHolder.java": """package org.apache.ignite.internal.processors.query;
                class QueryParserMetricsHolder {
                    static final String GROUP = "sql.parser.cache";
                    private final LongAdderMetric hits;
                    QueryParserMetricsHolder(GridMetricManager metricMgr) {
                        MetricRegistryImpl registry = metricMgr.registry(GROUP);
                        hits = registry.longAdderMetric("hits", "help");
                    }
                }""",
            "SchemaManager.java": """package org.apache.ignite.internal.processors.query.schema.management;
                class SchemaManager {
                    static final String SCHEMAS = "schemas";
                    void start() { ctx.systemView().registerView(SCHEMAS, "help", new SqlSchemaViewWalker(), schemas.values()); }
                }""",
            "IgniteOwnSqlOperatorTable.java": """package org.apache.ignite.internal.processors.query.calcite.sql.fun;
                class IgniteOwnSqlOperatorTable {
                    public static final SqlFunction LENGTH = new SqlFunction("LENGTH", SqlKind.OTHER_FUNCTION);
                }"""
        }
        after = {path: src.replace('"baselineAutoAdjustTimeout"', '"adjustTimeout"')
                 .replace('"sql.parser.cache"', '"sql.cache"').replace('"hits"', '"cacheHits"')
                 .replace('"schemas"', '"sqlSchemas"').replace('"LENGTH"', '"STRLEN"') for path, src in before.items()}
        self.assertEqual(sorted(compare(before, after)), sorted([
            "DistributedBaselineConfiguration.java\tChanged distributed setting name from baselineAutoAdjustTimeout to adjustTimeout",
            "QueryParserMetricsHolder.java\tChanged metric registry name from sql.parser.cache to sql.cache",
            "QueryParserMetricsHolder.java\tChanged metric name from hits to cacheHits",
            "SchemaManager.java\tChanged system view name from schemas to sqlSchemas",
            "IgniteOwnSqlOperatorTable.java\tChanged Calcite SQL function name from LENGTH to STRLEN",
        ]))

    def test_skips_internal_names_and_unregistered_properties(self):
        """Changing a private binding, an internal SQL function or an unregistered setting leaves the contract unchanged."""
        properties = """package org.apache.ignite.internal.cluster;
            class DistributedBaselineConfiguration {
                private final DistributedChangeableProperty<Long> timeout = detachedLongProperty("timeout", "help");
                private final DistributedChangeableProperty<Long> spare = detachedLongProperty("unused", "help");
                void init(DistributedPropertyDispatcher dispatcher) { dispatcher.registerProperties(timeout); }
            }"""
        sql = """package org.apache.ignite.internal.processors.query.calcite.sql.fun;
            class IgniteOwnSqlOperatorTable {
                public static final SqlFunction LENGTH = new SqlFunction("LENGTH", SqlKind.OTHER_FUNCTION);
                public static final SqlFunction NULL_BOUND = new SqlFunction("$NULL_BOUND", SqlKind.OTHER_FUNCTION);
            }"""
        before = {"DistributedBaselineConfiguration.java": properties, "IgniteOwnSqlOperatorTable.java": sql}
        after = {path: src.replace(" timeout ", " delay ").replace("Properties(timeout)", "Properties(delay)")
                 .replace('"unused"', '"other"').replace("NULL_BOUND", "BOUND").replace('"LENGTH"', '"length"')
                 for path, src in before.items()}
        self.assertEqual(compare(before, after), [])

    def test_adding_and_removing_a_property_in_an_existing_dispatcher(self):
        """A private setting becomes available when its field joins the dispatcher's registration list."""
        source = """package org.apache.ignite.internal.cluster;
            class DistributedBaselineConfiguration {
                private final DistributedChangeableProperty<Long> delay = detachedLongProperty("delay", "help");
                private final DistributedChangeableProperty<Long> threshold = detachedLongProperty("threshold", "help");
                void init(DistributedPropertyDispatcher dispatcher) { dispatcher.registerProperties(delay); }
            }"""
        path = "DistributedBaselineConfiguration.java"
        old = {path: source}
        new = {path: source.replace("registerProperties(delay)", "registerProperties(delay, threshold)")}
        self.assertEqual(compare(old, new), [path + "\tAdded distributed setting registration for threshold"])
        self.assertEqual(compare(new, old), [path + "\tRemoved distributed setting registration for threshold"])

    def test_native_parser_uses_keyword_values_and_skips_unknown_registrations(self):
        """Changing the reserved words matters, including when Java field names stay the same."""
        source = """package org.apache.ignite.internal.sql;
            class SqlKeyword {
                public static final String SHOW = "SHOW";
                private static final HashSet<String> KEYWORDS;
                static {
                    KEYWORDS = new HashSet<>();
                    try {
                        for (Field field : SqlKeyword.class.getDeclaredFields()) {
                            if (Objects.equals(String.class, field.getType())) {
                                String word = (String)field.get(null);
                                KEYWORDS.add(word);
                            }
                        }
                    } catch (ReflectiveOperationException e) { throw new IgniteException(e); }
                }
                public static boolean isKeyword(String str) { return KEYWORDS.contains(str); }
            }"""
        path = "SqlKeyword.java"
        old = {path: source}
        renamed = {path: source.replace('SHOW = "SHOW"', 'SHOW = "DISPLAY"')}
        self.assertEqual(compare(old, renamed), [
            path + "\tRemoved Ignite SQL parser keyword SHOW",
            path + "\tAdded Ignite SQL parser keyword DISPLAY",
        ])
        for edit in (source.replace('SHOW =', 'DISPLAY ='), source.replace('"SHOW"', 'configuredName()'),
                     renamed[path].replace("KEYWORDS.add(word);", "if (enabled(word)) KEYWORDS.add(word);")):
            self.assertEqual(compare(old, {path: edit}), [])
