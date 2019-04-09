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
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheDeleteSqlQuerySelfTest extends IgniteCacheAbstractSqlDmlQuerySelfTest {
    @Before
    public void cleanup() {
        execute("DROP TABLE IF EXISTS TEST_TABLE;");
    }

    /**
     *
     */
    @Test
    public void testDeleteSimple() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("delete from Person p where length(p._key) = 2 " +
            "or p.secondName like '%ite'"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(2, leftovers.size());

        assertEqualsCollections(asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(0));

        assertEqualsCollections(asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(1));
    }

    /**
     *
     */
    @Test
    public void testDeleteSingle() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("delete from Person where _key = ?")
            .setArgs("FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by id, _key"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(3, leftovers.size());

        assertEqualsCollections(asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(0));

        assertEqualsCollections(asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(1));

        assertEqualsCollections(asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));
    }

    /**
     * In binary mode, this test checks that inner forcing of keepBinary works - without it, EntryProcessors inside DML
     * engine would compare binary and non-binary objects with the same keys and thus fail.
     */
    @Test
    public void testDeleteSimpleWithoutKeepBinary() {
        IgniteCache p = ignite(0).cache("S2P");

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("delete from Person p where length(p._key) = 2 " +
            "or p.secondName like '%ite'"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(2, leftovers.size());

        assertEqualsCollections(asList("SecondKey", new Person(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(0));

        assertEqualsCollections(asList("f0u4thk3y", new Person(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(1));
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

        Stream.of(1, 1L, /*1d,*/ "1", "1.0", new BigDecimal("1.0"), new BigDecimal("1"))
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
            "PRIMARY KEY (ID)) with \"template=partitioned\"");

        Object one = 1.0d;

        execute("DELETE FROM TEST_TABLE");

        execute("INSERT INTO TEST_TABLE (id, value) VALUES (1, 'this row should be deleted'), (2, 'value')");

       // List<List<?>> actual = toList(executeH2("SELECT ID, VALUE FROM PUBLIC.TEST_TABLE WHERE ID = 1"));

        List<List<?>> actual = execute("SELECT\n" +
            "_KEY,\n" +
            "VALUE\n" +
            "FROM PUBLIC.TEST_TABLE\n" +
            "WHERE (ID = ?1) AND (VALUE = 'this row should be deleted')", one);
        List<List<?>> expRows = Collections.singletonList(asList(new BigDecimal(1), "this row should be deleted"));

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
}
