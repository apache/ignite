/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlTableFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteSqlSemantics;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** Tests SQL semantics that treats empty string as {@code null}. */
public class EmptyStringIsNullIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new AbstractTestPluginProvider() {
                /** {@inheritDoc} */
                @Override public String name() {
                    return "Empty string is null semantics";
                }

                /** {@inheritDoc} */
                @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                    if (FrameworkConfig.class.equals(cls)) {
                        FrameworkConfig cfg = Frameworks.newConfigBuilder(CalciteQueryProcessor.FRAMEWORK_CONFIG)
                            .context(Contexts.chain(
                                CalciteQueryProcessor.FRAMEWORK_CONFIG.getContext(),
                                Contexts.of(IgniteSqlSemantics.builder()
                                    .emptyStringIsNull(true)
                                    .build())))
                            .build();

                        return (T)cfg;
                    }

                    return super.createComponent(ctx, cls);
                }
            });
    }

    /** */
    @Test
    public void testLiteralsAndComparisons() {
        assertQuery("SELECT '', '' IS NULL, '' IS NOT NULL, COALESCE('', 'fallback')")
            .returns(null, true, false, "fallback")
            .check();

        assertQuery("SELECT '' = '', 'value' = '', 'value' <> ''")
            .returns(null, null, null)
            .check();

        assertQuery("SELECT CAST(? AS VARCHAR) IS NULL")
            .withParams("")
            .returns(true)
            .check();
    }

    /** */
    @Test
    public void testStorage() {
        sql("CREATE TABLE empty_string_test(id INT PRIMARY KEY, val VARCHAR)");

        sql("INSERT INTO empty_string_test VALUES (1, ''), (2, 'value'), (3, ?)", "");

        assertQuery("SELECT id, val, val IS NULL FROM empty_string_test ORDER BY id")
            .returns(1, null, true)
            .returns(2, "value", false)
            .returns(3, null, true)
            .check();

        assertQuery("SELECT id FROM empty_string_test WHERE val = '' OR val <> ''")
            .resultSize(0)
            .check();
    }

    /** */
    @Test
    public void testNotNullConstraint() {
        sql("CREATE TABLE empty_string_not_null_test(id INT PRIMARY KEY, val VARCHAR NOT NULL)");

        assertThrows("INSERT INTO empty_string_not_null_test VALUES (1, '')", IgniteSQLException.class,
            "Null value is not allowed");
        assertThrows("INSERT INTO empty_string_not_null_test VALUES (2, ?)", IgniteSQLException.class,
            "Null value is not allowed", "");
    }

    /** */
    @Test
    public void testExpressionAndAggregateResults() {
        assertQuery("SELECT LTRIM('     '), RTRIM('     '), TRIM('     '), REPEAT('value', -1)")
            .returns(null, null, null, null)
            .check();

        assertQuery("SELECT REPLACE('11', '1', ''), STRING_AGG('', '')")
            .returns(null, null)
            .check();

        assertQuery("SELECT '' ~ '.*', '' ~* '.*', '' !~ '.*', '' !~* '.*', 'value' ~ ''")
            .returns(null, null, null, null, null)
            .check();

        assertThrows("SELECT '' ~ '[a-z'", IgniteSQLException.class, null);
    }

    /** */
    @Test
    public void testUdfs() {
        client.getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(Functions.class));

        assertQuery("SELECT STRINGISNULL(''), STRINGISNULL(?), STRINGISNULL(CAST(? AS VARCHAR)), EMPTYSTRING()")
            .withParams("", "")
            .returns(true, true, true, null)
            .check();

        assertQuery("SELECT * FROM STRINGNULLS('')")
            .returns(true, null)
            .check();
    }

    /** */
    public static class Functions {
        /** */
        @QuerySqlFunction
        public static boolean stringIsNull(String val) {
            return val == null;
        }

        /** */
        @QuerySqlFunction
        public static String emptyString() {
            return "";
        }

        /** */
        @QuerySqlTableFunction(
            columnTypes = {boolean.class, String.class},
            columnNames = {"INPUT_IS_NULL", "EMPTY_RESULT"}
        )
        public static Collection<List<?>> stringNulls(String val) {
            return List.of(Arrays.asList(val == null, ""));
        }
    }
}
