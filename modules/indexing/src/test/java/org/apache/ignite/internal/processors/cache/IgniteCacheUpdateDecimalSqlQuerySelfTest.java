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

package org.apache.ignite.internal.processors.cache;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheUpdateDecimalSqlQuerySelfTest extends GridCommonAbstractTest {
    @Before
    public void cleanup() throws  Exception{
        startGridsMultiThreaded(3);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        execute("DROP TABLE IF EXISTS TEST_TABLE;");
    }

    protected IgniteCache<?,?> cache() {
        return grid(0).cache(DEFAULT_CACHE_NAME);
    }


    /**
     * Check delete type conversions in case of delete by "key = ?" and decimal key.
     */
    @Test
    public void testDeleteDecimalByKey() {
        execute("CREATE TABLE TEST_TABLE (" +
            "ID DECIMAL(19,1)," +
            "VALUE VARCHAR2(255 CHAR)," +
            "PRIMARY KEY (ID))");

        Stream.of(1, 1L, 1d, "1", "1.0", new BigDecimal("1.0"), new BigDecimal("1"))
            .forEach((Object one) -> {
                execute("DELETE FROM TEST_TABLE");

                execute("Insert INTO TEST_TABLE (id, value) VALUES (1, 'this row should be deleted'), (2, 'value')");

                execute("DELETE FROM TEST_TABLE WHERE ID = ?", one);

                List<List<?>> expRows = Collections.singletonList(asList(new BigDecimal(2), "value"));

                assertEqualsCollections("Argument of class " + one.getClass().getSimpleName() + " is converted incorrectly",
                    expRows, execute("SELECT * FROM TEST_TABLE ORDER BY ID"));
            });
    }

    /**
     * Check fast delete type conversions in case of delete by "key = ?".
     */
    @Test
    public void testFastDeleteByKey() {
        execute("CREATE TABLE TEST_TABLE (" +
            "ID DOUBLE," +
            "VALUE VARCHAR2(255 CHAR)," +
            "PRIMARY KEY (ID))");

        Stream.of(1, 1L, 1d, "1", "1.0", new BigDecimal("1.0"), new BigDecimal("1"))
            .forEach((Object one) -> {
                execute("DELETE FROM TEST_TABLE");

                execute("Insert INTO TEST_TABLE (id, value) VALUES (1, 'this row should be deleted'), (2, 'value')");

                execute("DELETE FROM TEST_TABLE WHERE ID = ?", one);

                List<List<?>> expRows = Collections.singletonList(asList(2.0, "value"));

                assertEqualsCollections("Argument of class " + one.getClass().getSimpleName() + " is converted incorrectly",
                    expRows, execute("SELECT * FROM TEST_TABLE ORDER BY ID"));
            });
    }

    /**
     * Check delete type conversions in case of delete by "key = ? and val = ?" and decimal key.
     */
    @Test
    public void testDeleteDecimalByKeyAndValue() {
        execute("CREATE TABLE TEST_TABLE (" +
            "ID DECIMAL(19,1)," +
            "VALUE VARCHAR2(255 CHAR)," +
            "PRIMARY KEY (ID))");

        Stream.of(1, 1L, 1d, "1", "1.0", new BigDecimal("1.0"), new BigDecimal("1"))
            .forEach((Object one) -> {
                execute("DELETE FROM TEST_TABLE");

                execute("Insert INTO TEST_TABLE (id, value) VALUES (1, 'this row should be deleted'), (2, 'value')");

                execute("DELETE FROM TEST_TABLE WHERE ID = ? AND VALUE = 'this row should be deleted'", one);

                List<List<?>> expRows = Collections.singletonList(asList(new BigDecimal(2), "value"));

                assertEqualsCollections("Argument of class " + one.getClass().getSimpleName() + " is converted incorrectly",
                    expRows, execute("SELECT * FROM TEST_TABLE ORDER BY ID"));
            });
    }

    /**
     * Check delete type conversions in case of delete by "key = ? and val = ?" and decimal key.
     */
    @Test
    public void testDeleteDecimalByKeyAndValueDOUBLE() {
        execute("CREATE TABLE TEST_TABLE (" +
            "ID DECIMAL(19,1)," +
            "VALUE VARCHAR2(255 CHAR)," +
            "PRIMARY KEY (ID))with \"template=partitioned\"");

        Object one = 1.0d;

        execute("DELETE FROM TEST_TABLE");

        execute("INSERT INTO TEST_TABLE (id, value) VALUES (1, 'this row should be deleted'), (2, 'value')");

        execute("DELETE FROM TEST_TABLE WHERE ID = ? AND VALUE = 'this row should be deleted'", one);

        List<List<?>> expRows = Collections.singletonList(asList(new BigDecimal(2), "value"));

        assertEqualsCollections("Argument of class " + one.getClass().getSimpleName() + " is converted incorrectly",
            expRows, execute("SELECT * FROM TEST_TABLE ORDER BY ID"));
    }

    /**
     * Check delete type conversions in case of delete by "key = ? and val = ?" and decimal key.
     */
    @Test
    public void testDeleteDecimalByKeyAndValueSelect() {
        execute("CREATE TABLE TEST_TABLE (" +
            "ID DECIMAL(19, 1)," +
            "VALUE VARCHAR2(255 CHAR)," +
            "PRIMARY KEY (ID)) with \"template=partitioned, wrap_value=false\"");

        Object one = 1.0d;

        IgniteCache<Object, Object> tab = grid(0).cache("SQL_PUBLIC_TEST_TABLE");
        tab.clear();
        tab.put(new BigDecimal("1"), "this row should be deleted");
        tab.put(new BigDecimal("2"), "value");

        List<List<?>> actual = execute("SELECT\n" +
            "ID,\n" +
            "VALUE\n" +
            "FROM PUBLIC.TEST_TABLE\n" +
//            "WHERE (ID >= ?1 and ID <= ?1)", new BigDecimal("1.00000"));
            "WHERE ID = ?1 ", new BigDecimal("1.0000"));

        List<List<?>> expRows = Collections.singletonList(asList(new BigDecimal(1), "this row should be deleted"));

        assertEqualsCollections("Argument of class " + one.getClass().getSimpleName() + " is converted incorrectly",
            expRows, actual);
    }


    @Test
    public void testDeleteDecimalInlineIndex() {
        execute("CREATE TABLE TEST_TABLE (" +
            "ID DECIMAL(19, 1)," +
            "VALUE DECIMAL(19,1)," +
            "PRIMARY KEY (ID)) with \"template=partitioned, wrap_value=false, wrap_key=true, key_type=" + DecWrapper.class.getName() + "\"");

        Object one = 1.0d;

        IgniteCache<Object, Object> tab = grid(0).cache("SQL_PUBLIC_TEST_TABLE");

        tab.clear();

        tab.put(new DecWrapper("1"), new BigDecimal("42"));
        tab.put(new DecWrapper("2"), new BigDecimal("77"));

        List<List<?>> actual = execute("SELECT\n" +
            "ID,\n" +
            "VALUE\n" +
            "FROM PUBLIC.TEST_TABLE\n" +
//            "WHERE (ID >= ?1 and ID <= ?1)", new BigDecimal("1.00000"));
            "WHERE id = ?", new BigDecimal("1.0000"));
        List<List<?>> expRows = Collections.singletonList(asList(new BigDecimal(1), new BigDecimal(42)));

        assertEqualsCollections("Argument of class " + one.getClass().getSimpleName() + " is converted incorrectly",
            expRows, actual);
    }

    /**
     * Check fast delete type conversions in case of delete by "key = ? and val = ?".
     */
    @Test
    public void testFastDeleteByKeyAndValue() {
        execute("CREATE TABLE TEST_TABLE (" +
            "ID DOUBLE," +
            "VALUE VARCHAR2(255 CHAR)," +
            "PRIMARY KEY (ID))");

        Stream.of(1, 1L, 1d,"1", "1.0", new BigDecimal("1.0"), new BigDecimal("1"))
            .forEach((Object one) -> {
                execute("DELETE FROM TEST_TABLE");

                execute("Insert INTO TEST_TABLE (id, value) VALUES (1, 'this row should be deleted'), (2, 'value')");

                execute("DELETE FROM TEST_TABLE WHERE ID = ? AND VALUE = 'this row should be deleted'", one);

                List<List<?>> expRows = Collections.singletonList(asList(2.0, "value"));

                assertEqualsCollections("Argument of class " + one.getClass().getSimpleName() + " is converted incorrectly",
                    expRows, execute("SELECT * FROM TEST_TABLE ORDER BY ID"));
            });
    }

    /**
     * Execute sql query using cache API.
     *
     * @param sql query.
     * @return fetched result.
     */
    private List<List<?>> execute(String sql, Object... args) {
        return cache().query(new SqlFieldsQuery(sql).setArgs(args).setSchema("PUBLIC")).getAll();
    }

    /**
     * Execute sql query using cache API.
     *
     * @param sql query.
     * @return fetched result.
     */
    private List<List<?>> executeLocal(String sql, Object... args) {
        return cache().query(new SqlFieldsQuery(sql).setLocal(true).setArgs(args).setSchema("PUBLIC")).getAll();
    }


    private ResultSet executeH2(String sql) {
        IgniteH2Indexing idx = (IgniteH2Indexing)grid(0).context().query().getIndexing();

        PreparedStatement stmt = idx.prepareNativeStatement("PUBLIC", sql);

        try {
            return stmt.executeQuery();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private static List<List<?>> toList(ResultSet rs) {
        try {
            ResultSetMetaData meta = rs.getMetaData();

            ArrayList tab = new ArrayList<>();

            while (rs.next()) {
                ArrayList row = new ArrayList<>(meta.getColumnCount());

                for (int i = 1; i <= meta.getColumnCount(); i++)
                    row.add(rs.getObject(i));

                tab.add(row);
            }

            return tab;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static class DecWrapper{
        public BigDecimal id;

        public DecWrapper(String idStr) {
            id = new BigDecimal(idStr);
        }

        public DecWrapper() {

        }
    }
}
