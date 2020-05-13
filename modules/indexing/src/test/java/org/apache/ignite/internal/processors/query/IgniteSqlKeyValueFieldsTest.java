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

package org.apache.ignite.internal.processors.query;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test hidden _key, _val, _ver columns
 */
public class IgniteSqlKeyValueFieldsTest extends AbstractIndexingCommonTest {
    /** */
    private static String NODE_BAD_CONF_MISS_KEY_FIELD = "badConf1";

    /** */
    private static String NODE_BAD_CONF_MISS_VAL_FIELD = "badConf2";

    /** */
    private static String NODE_CLIENT = "client";

    /** */
    private static String CACHE_PERSON_NO_KV = "PersonNoKV";

    /** */
    private static String CACHE_INT_NO_KV_TYPE = "IntNoKVType";

    /** */
    private static String CACHE_PERSON = "Person";

    /** */
    private static String CACHE_JOB = "Job";

    /**  */
    private static String CACHE_SQL = "Sql";

    /** */
    private boolean oldAllowKeyValCol;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setMarshaller(new BinaryMarshaller());

        List<CacheConfiguration> ccfgs = new ArrayList<>();
        CacheConfiguration ccfg = buildCacheConfiguration(gridName);
        if (ccfg != null)
            ccfgs.add(ccfg);

        ccfgs.add(buildCacheConfiguration(CACHE_PERSON_NO_KV));
        ccfgs.add(buildCacheConfiguration(CACHE_INT_NO_KV_TYPE));
        ccfgs.add(buildCacheConfiguration(CACHE_PERSON));
        ccfgs.add(buildCacheConfiguration(CACHE_JOB));
        ccfgs.add(buildCacheConfiguration(CACHE_SQL));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        oldAllowKeyValCol = GridTestUtils.getFieldValue(UpdatePlanBuilder.class,
            UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES");

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        startGrid(0);
        startClientGrid(NODE_CLIENT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", oldAllowKeyValCol);

        super.afterTest();
    }

    private CacheConfiguration buildCacheConfiguration(String name) {
        if (name.equals(NODE_BAD_CONF_MISS_KEY_FIELD)) {
            CacheConfiguration ccfg = new CacheConfiguration(NODE_BAD_CONF_MISS_KEY_FIELD);
            QueryEntity qe = new QueryEntity(Object.class.getName(), Object.class.getName());
            qe.setKeyFieldName("k");
            qe.addQueryField("a", Integer.class.getName(), null);
            ccfg.setQueryEntities(F.asList(qe));
            return ccfg;
        }
        else if (name.equals(NODE_BAD_CONF_MISS_VAL_FIELD)) {
            CacheConfiguration ccfg = new CacheConfiguration(NODE_BAD_CONF_MISS_VAL_FIELD);
            QueryEntity qe = new QueryEntity(Object.class.getName(), Object.class.getName());
            qe.setValueFieldName("v");
            qe.addQueryField("a", Integer.class.getName(), null);
            ccfg.setQueryEntities(F.asList(qe));
            return ccfg;
        }
        else if (name.equals(CACHE_PERSON_NO_KV)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_PERSON_NO_KV);

            QueryEntity entity = new QueryEntity();

            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Person.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("name", String.class.getName());
            fields.put("age", Integer.class.getName());

            entity.setFields(fields);

            ccfg.setQueryEntities(asList(entity));
            return ccfg;
        }
        else if (name.equals(CACHE_INT_NO_KV_TYPE)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_INT_NO_KV_TYPE);
            QueryEntity entity = new QueryEntity();

            entity.setKeyType(null);
            entity.setValueType(null);

            entity.setKeyFieldName("id");
            entity.setValueFieldName("v");

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("id", Integer.class.getName());
            fields.put("v", Integer.class.getName());

            entity.setFields(fields);

            ccfg.setQueryEntities(asList(entity));
            return ccfg;
        }
        else if (name.equals(CACHE_PERSON)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_PERSON);

            QueryEntity entity = new QueryEntity();

            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Person.class.getName());

            entity.setKeyFieldName("id");
            entity.setValueFieldName("v");

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("name", String.class.getName());
            fields.put("age", Integer.class.getName());

            fields.put(entity.getKeyFieldName(), entity.getKeyType());
            fields.put(entity.getValueFieldName(), entity.getValueType());

            entity.setFields(fields);

            ccfg.setQueryEntities(asList(entity));
            return ccfg;
        }
        else if (name.equals(CACHE_JOB)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_JOB);
            ccfg.setIndexedTypes(Integer.class, Integer.class);
            return ccfg;
        }
        else if (name.equals(CACHE_SQL))
            return new CacheConfiguration<>(name).setSqlSchema(DFLT_SCHEMA);

        return null;
    }

    /** Test for setIndexedTypes() primitive types */
    @Test
    public void testSetIndexTypesPrimitive() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(NODE_CLIENT).cache(CACHE_JOB);

        checkInsert(cache, "insert into Integer (_key, _val) values (?,?)", 1, 100);

        checkSelect(cache, "select * from Integer", 1, 100);
        checkSelect(cache, "select _key, _val from Integer", 1, 100);
    }

    /** Test configuration error : keyFieldName is missing from fields */
    @Test
    public void testErrorKeyFieldMissingFromFields() throws Exception {
        checkCacheStartupError(NODE_BAD_CONF_MISS_KEY_FIELD);
    }

    /** Test configuration error : valueFieldName is missing from fields */
    @Test
    public void testErrorValueFieldMissingFromFields() throws Exception {
        checkCacheStartupError(NODE_BAD_CONF_MISS_VAL_FIELD);
    }

    /** */
    private void checkCacheStartupError(final String name) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(name);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * Check that it is allowed to leave QE.keyType and QE.valueType unset
     * in case keyFieldName and valueFieldName are set and present in fields
     */
    @Test
    public void testQueryEntityAutoKeyValTypes() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(NODE_CLIENT).cache(CACHE_INT_NO_KV_TYPE);

        checkInsert(cache, "insert into Integer (_key, _val) values (?,?)", 1, 100);

        checkSelect(cache, "select * from Integer where id = 1", 1, 100);

        checkSelect(cache, "select * from Integer", 1, 100);
        checkSelect(cache, "select _key, _val from Integer", 1, 100);
        checkSelect(cache, "select id, v from Integer", 1, 100);
    }

    /** Check that it is possible to not have keyFieldName and valueFieldName */
    @Test
    public void testNoKeyValueAliases() throws Exception {
        IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_NO_KV);

        Person alice = new Person("Alice", 1);
        checkInsert(cache, "insert into Person (_key, _val) values (?,?)", 1, alice);

        checkSelect(cache, "select * from Person", alice.name, alice.age);
        checkSelect(cache, "select _key, _val from Person", 1, alice);
    }

    /** Check keyFieldName and valueFieldName columns access */
    @Test
    public void testKeyValueAlias() throws Exception {
        //_key, _val, _ver | name, age, id, v
        Person alice = new Person("Alice", 1);
        Person bob = new Person("Bob", 2);

        IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        checkInsert(cache, "insert into Person (_key, _val) values (?,?)", 1, alice);
        checkInsert(cache, "insert into Person (id, v) values (?,?)", 2, bob);

        checkSelect(cache, "select * from Person where _key=1", alice.name, alice.age, 1, alice);
        checkSelect(cache, "select _key, _val from Person where id=1", 1, alice);

        checkSelect(cache, "select * from Person where _key=2", bob.name, bob.age, 2, bob);
        checkSelect(cache, "select _key, _val from Person where id=2", 2, bob);

        checkInsert(cache, "update Person set age = ? where id = ?", 3, 1);
        checkSelect(cache, "select _key, age from Person where id=1", 1, 3);

        checkInsert(cache, "update Person set v = ? where id = ?", alice, 1);
        checkSelect(cache, "select _key, _val from Person where id=1", 1, alice);
    }

    /** Check that joins are working on keyFieldName, valueFieldName columns */
    @Test
    public void testJoinKeyValFields() throws Exception {
        IgniteEx client = grid(NODE_CLIENT);
        IgniteCache<Integer, Person> cache = client.cache(CACHE_PERSON);
        IgniteCache<Integer, Integer> cache2 = client.cache(CACHE_JOB);

        checkInsert(cache, "insert into Person (id, v) values (?, ?)", 1, new Person("Bob", 30));
        checkInsert(cache, "insert into Person (id, v) values (?, ?)", 2, new Person("David", 35));
        checkInsert(cache2, "insert into Integer (_key, _val) values (?, ?)", 100, 1);
        checkInsert(cache2, "insert into Integer (_key, _val) values (?, ?)", 200, 2);

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select p.id, j._key from Person p, \"" + CACHE_JOB + "\".Integer j where p.id = j._val"));
        List<List<?>> results = cursor.getAll();
        assertEquals(2, results.size());
        assertEquals(1, results.get(0).get(0));
        assertEquals(100, results.get(0).get(1));
        assertEquals(2, results.get(1).get(0));
        assertEquals(200, results.get(1).get(1));
    }

    /** Check automatic addition of index for keyFieldName column */
    @Test
    public void testAutoKeyFieldIndex() throws Exception {
        IgniteEx client = grid(NODE_CLIENT);
        IgniteCache<Integer, Person> cache = client.cache(CACHE_PERSON);

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select * from Person where id = 1"));
        List<List<?>> results = cursor.getAll();
        assertEquals(1, results.size());
        assertTrue(((String)results.get(0).get(0)).contains("\"_key_PK_proxy\""));

        cursor = cache.query(new SqlFieldsQuery("explain select * from Person where _key = 1"));
        results = cursor.getAll();
        assertEquals(1, results.size());
        assertTrue(((String)results.get(0).get(0)).contains("\"_key_PK\""));
    }

    /**
     * Test to verify that there will be an error when changing the column type
     * through sql "alter table" drop and then add column, and also when trying
     * to create an index, there will be an error.
     */
    @Test
    public void testChangeColumnTypeByAlterTableDropAddColumn() {
        // test1
        changeSqlColumnType(
            "int",
            1,
            asList("varchar", "date", "tinyint", "long", "datetime"),
            new LinkedList<>(asList("a", "b,a", "a", "a,b", "a"))
        );

        // test2
        changeSqlColumnType(
            "varchar",
            "1",
            asList("int", "date", "tinyint", "long", "datetime"),
            new LinkedList<>(asList("a", "b,a", "a", "a,b", "a"))
        );

        // test3
        changeSqlColumnType(
            "date",
            new Timestamp(0),
            asList("int", "long", "tinyint", "datetime", "varchar"),
            new LinkedList<>(asList("a", "b,a", "a,b", "a", "a"))
        );

        // test4
        changeSqlColumnType(
            "datetime",
            new Timestamp(0),
            asList("int", "long", "tinyint", "date", "varchar"),
            new LinkedList<>(asList("a", "b,a", "a,b", "a", "a"))
        );
    }

    /**
     * Test to verify that when changing column type, there will be no error
     * without changing type through sql "alter table" drop and then add column
     * and also when trying to create an index there will be no error.
     */
    @Test
    public void testReturnColumnTypeByAlterTableDropAddColumn() {
        checkRecreateSqlColumn("int", 1, asList("int"), new LinkedList<>(asList("a")));

        checkRecreateSqlColumn("varchar", 1, asList("varchar"), new LinkedList<>(asList("b,a")));

        checkRecreateSqlColumn("date", new java.sql.Date(0), asList("date"), new LinkedList<>(asList("b")));

        checkRecreateSqlColumn("tinyint", 1, asList("tinyint"), new LinkedList<>(asList("a,b")));

        checkRecreateSqlColumn("long", 1L, asList("long"), new LinkedList<>(asList("a")));

        checkRecreateSqlColumn("datetime", new Timestamp(0), asList("datetime"), new LinkedList<>(asList("a")));
    }

    /**
     * Verifies that when changing a sql column type by sql "alter table"
     * drop column and then add column, an exception will be thrown and also
     * after that when trying to create an index on this column
     * will lead to an error. Here used table
     * "create table TEST (id int primary key, a x, b int)", where all logic
     * is focused on column "a".
     *
     * @param aColType The initial type of column is "a".
     * @param aInsertVal The value for insert into "a" column.
     * @param aColTypes Column types for changing "a" column.
     * @param idxCols Columns (single or comma-separated) to create indexes.
     */
    private void changeSqlColumnType(
        String aColType,
        Object aInsertVal,
        List<String> aColTypes,
        Queue<String> idxCols
    ) {
        assert nonNull(aInsertVal);
        assert nonNull(aColType);
        assert nonNull(aColTypes);
        assert nonNull(idxCols);

        IgniteCache<Object, Object> cache = grid(0).cache(CACHE_SQL);

        cache.query(
            new SqlFieldsQuery("create table TEST (id int primary key, a " + aColType + ", b int)")
        ).getAll();

        cache.query(new SqlFieldsQuery("insert into TEST (id, a, b) VALUES (?,?,?)").setArgs(1, aInsertVal, 1))
            .getAll();

        for (String columnType : aColTypes) {
            cache.query(new SqlFieldsQuery("alter table TEST drop column a")).getAll();

            assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery("alter table TEST add column a " + columnType)).getAll(),
                CacheException.class,
                "Column already exists: with a different type."
            );

            assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery("create index tidx on TEST (" + idxCols.poll() + ")")).getAll(),
                CacheException.class,
                "Column doesn't exist: A"
            );

            cache.query(new SqlFieldsQuery("alter table TEST add column a " + aColType)).getAll();
        }

        cache.query(new SqlFieldsQuery("DROP TABLE TEST")).getAll();
    }

    /**
     * Checking that when using sql "alter table" drop column and then add
     * column without changing the column type will not throw an exception
     * and you can also create indexes without errors. Here used table
     * "create table TEST (id int primary key, a x, b int)", where all logic
     * is focused on column "a".
     *
     * @param aColType The initial type of column is "a".
     * @param aInsertVal The value for insert into "a" column.
     * @param aColTypes Column types for changing "a" column.
     * @param idxCols Columns (single or comma-separated) to create indexes.
     */
    private void checkRecreateSqlColumn(
        String aColType,
        Object aInsertVal,
        List<String> aColTypes,
        Queue<String> idxCols
    ) {
        assert nonNull(aInsertVal);
        assert nonNull(aColType);
        assert nonNull(aColTypes);
        assert nonNull(idxCols);

        IgniteCache<Object, Object> cache = grid(0).cache(CACHE_SQL);

        cache.query(
            new SqlFieldsQuery("create table TEST (id int primary key, a " + aColType + ", b int)")
        ).getAll();

        cache.query(new SqlFieldsQuery("insert into TEST (id, a, b) VALUES (?,?,?)").setArgs(1, aInsertVal, 1)).getAll();

        SqlFieldsQuery selectQry = new SqlFieldsQuery("select id,a,b from TEST");

        for (String columnType : aColTypes) {
            List<List<?>> rowsBeforeColManipulation = cache.query(selectQry).getAll();

            assertEquals(1, rowsBeforeColManipulation.size());
            assertEquals(3, rowsBeforeColManipulation.get(0).size());

            cache.query(new SqlFieldsQuery("alter table TEST drop column a")).getAll();
            cache.query(new SqlFieldsQuery("alter table TEST add column a " + columnType)).getAll();

            List<List<?>> rowsAfterColManipulation = cache.query(selectQry).getAll();

            assertEquals(1, rowsAfterColManipulation.size());
            assertEquals(rowsBeforeColManipulation.get(0), rowsAfterColManipulation.get(0));

            cache.query(new SqlFieldsQuery("create index tidx on TEST (" + idxCols.poll() + ")")).getAll();
        }

        cache.query(
            new SqlFieldsQuery("DROP TABLE TEST")
        ).getAll();
    }

    /** */
    private GridCacheVersion getVersion(IgniteCache<?, ?> cache, int key) {
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select _ver from Person where id = ?").setArgs(key));
        List<List<?>> results = cursor.getAll();
        assertEquals(1, results.size());
        return ((GridCacheVersion) results.get(0).get(0));
    }

    /** */
    private void checkInsert(IgniteCache<?, ?> cache, String qry, Object... args) throws Exception {
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(qry).setArgs(args));
        assertEquals(1, ((Number) cursor.getAll().get(0).get(0)).intValue());
    }

    /** */
    private void checkSelect(IgniteCache<?, ?> cache, String selectQry, Object... expected) {
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(selectQry));

        List<List<?>> results = cursor.getAll();

        assertEquals(1, results.size());

        List<?> row0 = results.get(0);
        for (int col = 0; col < expected.length; ++col)
            assertEquals(expected[col], row0.get(col));
    }

    /** */
    private static class Person {
        /** */
        private String name;

        /** */
        private int age;

        /** */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /** */
        @Override public int hashCode() {
            return name.hashCode() ^ age;
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Person))
                return false;
            Person other = (Person)o;
            return name.equals(other.name) && age == other.age;
        }
    }
}
