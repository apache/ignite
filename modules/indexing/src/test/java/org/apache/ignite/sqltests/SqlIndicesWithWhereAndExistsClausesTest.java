/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package org.apache.ignite.sqltests;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SqlIndicesWithWhereAndExistsClausesTest extends AbstractIndexingCommonTest {

    private static final int ROWS = 5_000;

    private static final int CLIENT = 10;

    private static final String SQL_SCHEMA = "ENTITY";

    private static final String EXPLAIN_SELECT = "explain select 1 from %s t";

    private static final String QUERY_COMPLEX_EXISTS = " where exists (select idx from %s " +
            "where t.idx = %d and t.val = %s)";

    private static final String QUERY_SIMPLE_EXISTS = " where exists (select idx from %s where t.idx = %d)";

    private static final String QUERY_SIMPLE_WHERE = " where t.idx = %d";

    private static final String QUERY_COMPLEX_WHERE = " where t.idx = %d and t.val = %s";

    private static final String USE_INDEX = " use index(%s)";

    private static final String GROUP_IDX = "id_val_idx";

    private static final String ID_IDX = "_idx_idx";

    private static final String VAL_IDX = "_val_idx";

    private static IgniteEx igniteClient;

    @Parameter
    public boolean hint;

    @Parameters(name = "{index}: hint - {0} ")
    public static Object[] data() {
        return new Object[] {false, true};
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(2);
        igniteClient = startClientGrid(CLIENT);
    }

    @Test
    public void testSingleIndexSimpleWhereClause() {
        createTestEntityTableWithAnIndex();

        String tableName = SingleIndexTestEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName) + String.format(QUERY_SIMPLE_WHERE, -5);

        runQuery(query);
    }

    @Test
    public void testSingleIndexSimpleExistsClause() {
        createTestEntityTableWithAnIndex();

        String tableName = SingleIndexTestEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName) + String.format(QUERY_SIMPLE_EXISTS, tableName, -5);

        runQuery(query);
    }

    @Test
    public void testComplexIndexComplexWhereClause() {
        createComplexEntityTableWithIndices();

        String tableName = ComplexIndexEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName, Collections.singleton(GROUP_IDX)) +
                String.format(QUERY_COMPLEX_WHERE, -5, "-5");

        runQuery(query);
    }

    @Test
    public void testComplexIndexComplexExistsClause() {
        createComplexEntityTableWithIndices();

        String tableName = ComplexIndexEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName, Collections.singleton(GROUP_IDX)) +
                String.format(QUERY_COMPLEX_EXISTS, tableName, -5, "-5");

        runQuery(query);
    }

    @Test
    public void testSingleIndexComplexWhereClause() {
        createTestEntityTableWithAnIndex();

        String tableName = SingleIndexTestEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName) + String.format(QUERY_COMPLEX_WHERE, -5, "-5");

        runQuery(query);
    }

    @Test
    public void testSingleIndexComplexExistsClause() {
        createTestEntityTableWithAnIndex();

        String tableName = SingleIndexTestEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName) + String.format(QUERY_COMPLEX_EXISTS, tableName, -5, "-5");

        runQuery(query);
    }

    @Test
    public void testDoubleIndexComplexExistsClause() {
        createEntityTableWithIndices();

        String tableName = DoubleIndexTestEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName, new HashSet<String>() {{
            add(tableName + ID_IDX);
            add(tableName + VAL_IDX);
        }}) + String.format(QUERY_COMPLEX_EXISTS, tableName, -5, "-5");

        runQuery(query);
    }

    @Test
    public void testDoubleIndexComplexWhereClause() {
        createEntityTableWithIndices();

        String tableName = DoubleIndexTestEntity.class.getSimpleName();
        String query = formInitialQueryPart(tableName, new HashSet<String>() {{
            add(tableName + ID_IDX);
            add(tableName + VAL_IDX);
        }}) + String.format(QUERY_COMPLEX_WHERE, -5, "-5");

        runQuery(query);
    }

    public void runQuery(String query) {

        List<List<?>> explained = getQueryResult(query);

        Assert.assertEquals(2, explained.size());
        String explainedString = (String) explained.get(0).get(0);

        Assert.assertTrue(explainedString, explainedString.contains("_IDX"));
        Assert.assertFalse(explainedString, explainedString.contains("SCAN"));
    }

    @NotNull
    private String formInitialQueryPart(String tableName) {
        return formInitialQueryPart(tableName, Collections.singleton(tableName + ID_IDX));
    }

    @NotNull
    private String formInitialQueryPart(String tableName, Set<String> indices) {
        return String.format(EXPLAIN_SELECT, tableName) +
                (hint ? String.format(USE_INDEX, String.join(",", indices)) : "");
    }

    private List<List<?>> getQueryResult(String query) {
        return igniteClient.context().query().querySqlFields(new SqlFieldsQuery(query)
                .setSchema(SQL_SCHEMA), false)
                .getAll();
    }

    private void createTestEntityTableWithAnIndex() {
        IgniteCache<Object, Object> cache = igniteClient.getOrCreateCache(
                new CacheConfiguration<>(SingleIndexTestEntity.class.getSimpleName())
                        .setIndexedTypes(Integer.class, SingleIndexTestEntity.class)
                        .setSqlSchema(SQL_SCHEMA)
        );

        populateTable(cache, SingleIndexTestEntity::new);
    }

    private void createComplexEntityTableWithIndices() {
        IgniteCache<Object, Object> cache = igniteClient.getOrCreateCache(
                new CacheConfiguration<>(ComplexIndexEntity.class.getSimpleName())
                        .setIndexedTypes(Integer.class, ComplexIndexEntity.class)
                        .setIndexedTypes(String.class, ComplexIndexEntity.class)
                        .setSqlSchema(SQL_SCHEMA)
        );

        populateTable(cache, ComplexIndexEntity::new);
    }

    private void createEntityTableWithIndices() {
        IgniteCache<Object, Object> cache = igniteClient.getOrCreateCache(
                new CacheConfiguration<>(DoubleIndexTestEntity.class.getSimpleName())
                        .setIndexedTypes(Integer.class, DoubleIndexTestEntity.class)
                        .setIndexedTypes(String.class, DoubleIndexTestEntity.class)
                        .setSqlSchema(SQL_SCHEMA)
        );

        populateTable(cache, DoubleIndexTestEntity::new);
    }

    private void populateTable(IgniteCache<Object, Object> cache, Function<Integer, ?> function) {
        IntStream
                .range(0, ROWS)
                .forEach(num -> cache.putIfAbsent(ThreadLocalRandom.current().nextInt(ROWS),
                        function.apply(num))
                );

        cache.put(-1, function.apply(-5));
    }

    private static class SingleIndexTestEntity {

        @QuerySqlField(index = true)
        private final int idx;

        @QuerySqlField
        private final String val;

        private SingleIndexTestEntity(int idx) {
            this.idx = idx;
            val = Integer.toString(idx);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SingleIndexTestEntity)) return false;
            SingleIndexTestEntity that = (SingleIndexTestEntity) o;
            return idx == that.idx && Objects.equals(val, that.val);
        }

        @Override public int hashCode() {
            return Objects.hash(idx, val);
        }

        @Override public String toString() {
            return "SingleIndexTestEntity{" +
                    "idx=" + idx +
                    ", val=" + val +
                    '}';
        }
    }

    private static class ComplexIndexEntity {

        @QuerySqlField(orderedGroups = { @QuerySqlField.Group(name = GROUP_IDX, order = 0) })
        private final int idx;

        @QuerySqlField(orderedGroups = { @QuerySqlField.Group(name = GROUP_IDX, order = 0) })
        private final String val;

        private final Object someObject;

        private ComplexIndexEntity(int idx) {
            this.idx = idx;
            val = Integer.toString(idx);
            someObject = new Object();
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ComplexIndexEntity)) return false;
            ComplexIndexEntity that = (ComplexIndexEntity) o;
            return idx == that.idx && val.equals(that.val) && someObject.equals(that.someObject);
        }

        @Override public int hashCode() {
            return Objects.hash(idx, val, someObject);
        }

        @Override public String toString() {
            return "ComplexIndexEntity{" +
                    "idx=" + idx +
                    ", val='" + val + '\'' +
                    ", someObject=" + someObject +
                    '}';
        }
    }

    private static class DoubleIndexTestEntity {

        @QuerySqlField(index = true)
        private final int idx;

        @QuerySqlField(index = true)
        private final String val;

        private DoubleIndexTestEntity(int idx) {
            this.idx = idx;
            val = Integer.toString(idx);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SingleIndexTestEntity)) return false;
            SingleIndexTestEntity that = (SingleIndexTestEntity) o;
            return idx == that.idx && Objects.equals(val, that.val);
        }

        @Override public int hashCode() {
            return Objects.hash(idx, val);
        }

        @Override public String toString() {
            return "SingleIndexTestEntity{" +
                    "idx=" + idx +
                    ", val=" + val +
                    '}';
        }
    }
}
