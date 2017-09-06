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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Test hidden _key, _val, _ver columns
 */
public class IgniteSqlKeyValueFieldsTest  extends GridCommonAbstractTest {

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new BinaryMarshaller());

        List<CacheConfiguration> ccfgs = new ArrayList<>();
        CacheConfiguration ccfg = buildCacheConfiguration(gridName);
        if (ccfg != null)
            ccfgs.add(ccfg);

        ccfgs.add(buildCacheConfiguration(CACHE_PERSON_NO_KV));
        ccfgs.add(buildCacheConfiguration(CACHE_INT_NO_KV_TYPE));
        ccfgs.add(buildCacheConfiguration(CACHE_PERSON));
        ccfgs.add(buildCacheConfiguration(CACHE_JOB));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));
        if (gridName.equals(NODE_CLIENT))
            c.setClientMode(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);
        startGrid(NODE_CLIENT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
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

            ccfg.setQueryEntities(Arrays.asList(entity));
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

            ccfg.setQueryEntities(Arrays.asList(entity));
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

            ccfg.setQueryEntities(Arrays.asList(entity));
            return ccfg;
        }
        else if (name.equals(CACHE_JOB)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_JOB);
            ccfg.setIndexedTypes(Integer.class, Integer.class);
            return ccfg;
        }
        return null;
    }

    /** Test for setIndexedTypes() primitive types */
    public void testSetIndexTypesPrimitive() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(NODE_CLIENT).cache(CACHE_JOB);

        checkInsert(cache, "insert into Integer (_key, _val) values (?,?)", 1, 100);

        checkSelect(cache, "select * from Integer", 1, 100);
        checkSelect(cache, "select _key, _val from Integer", 1, 100);
    }

    /** Test configuration error : keyFieldName is missing from fields */
    public void testErrorKeyFieldMissingFromFields() throws Exception {
        checkCacheStartupError(NODE_BAD_CONF_MISS_KEY_FIELD);
    }

    /** Test configuration error : valueFieldName is missing from fields */
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
    public void testQueryEntityAutoKeyValTypes() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(NODE_CLIENT).cache(CACHE_INT_NO_KV_TYPE);

        checkInsert(cache, "insert into Integer (_key, _val) values (?,?)", 1, 100);

        checkSelect(cache, "select * from Integer where id = 1", 1, 100);

        checkSelect(cache, "select * from Integer", 1, 100);
        checkSelect(cache, "select _key, _val from Integer", 1, 100);
        checkSelect(cache, "select id, v from Integer", 1, 100);
    }

    /** Check that it is possible to not have keyFieldName and valueFieldName */
    public void testNoKeyValueAliases() throws Exception {
        IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_NO_KV);

        Person alice = new Person("Alice", 1);
        checkInsert(cache, "insert into Person (_key, _val) values (?,?)", 1, alice);

        checkSelect(cache, "select * from Person", alice.name, alice.age);
        checkSelect(cache, "select _key, _val from Person", 1, alice);
    }

    /** Check keyFieldName and valueFieldName columns access */
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

    /** Check _ver version field is accessible */
    public void testVersionField() throws Exception {
        Person alice = new Person("Alice", 1);
        Person bob = new Person("Bob", 2);

        IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        checkInsert(cache, "insert into Person (id, v) values (?,?)", 1, alice);
        assertNotNull(getVersion(cache, 1));

        checkInsert(cache, "insert into Person (id, v) values (?,?)", 2, bob);
        assertNotNull(getVersion(cache, 2));

        GridCacheVersion v1 = getVersion(cache, 1);

        checkInsert(cache, "update Person set age = ? where id = ?", 3, 1);

        GridCacheVersion v2 = getVersion(cache, 1);

        assertFalse( v1.equals(v2) );
    }

    /** Check that joins are working on keyFieldName, valueFieldName columns */
    public void testJoinKeyValFields() throws Exception {
        IgniteEx client = grid(NODE_CLIENT);
        IgniteCache<Integer, Person> cache = client.cache(CACHE_PERSON);
        IgniteCache<Integer, Integer> cache2 = client.cache(CACHE_JOB);

        checkInsert(cache, "insert into Person (id, v) values (?, ?)", 1, new Person("Bob", 30));
        checkInsert(cache, "insert into Person (id, v) values (?, ?)", 2, new Person("David", 35));
        checkInsert(cache2, "insert into Integer (_key, _val) values (?, ?)", 100, 1);
        checkInsert(cache2, "insert into Integer (_key, _val) values (?, ?)", 200, 2);

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select p.id, j._key from Person p, \""+ CACHE_JOB +"\".Integer j where p.id = j._val"));
        List<List<?>> results = cursor.getAll();
        assertEquals(2, results.size());
        assertEquals(1, results.get(0).get(0));
        assertEquals(100, results.get(0).get(1));
        assertEquals(2, results.get(1).get(0));
        assertEquals(200, results.get(1).get(1));
    }

    /** Check automatic addition of index for keyFieldName column */
    public void testAutoKeyFieldIndex() throws Exception {
        IgniteEx client = grid(NODE_CLIENT);
        IgniteCache<Integer, Person> cache = client.cache(CACHE_PERSON);

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select * from Person where id = 1"));
        List<List<?>> results = cursor.getAll();
        assertEquals(2, results.size());
        assertTrue(((String)results.get(0).get(0)).contains("\"_key_PK_proxy\""));

        cursor = cache.query(new SqlFieldsQuery("explain select * from Person where _key = 1"));
        results = cursor.getAll();
        assertEquals(2, results.size());
        assertTrue(((String)results.get(0).get(0)).contains("\"_key_PK\""));
    }

    /** */
    private GridCacheVersion getVersion(IgniteCache<?, ?> cache, int key) {
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select _ver from Person where id = ?").setArgs(key));
        List<List<?>> results = cursor.getAll();
        assertEquals(1, results.size());
        return ((GridCacheVersion) results.get(0).get(0));
    }

    /** */
    private void checkInsert(IgniteCache<?, ?> cache, String qry, Object ... args) throws Exception {
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(qry).setArgs(args));
        assertEquals(1, ((Number) cursor.getAll().get(0).get(0)).intValue());
    }

    /** */
    private void checkSelect(IgniteCache<?, ?> cache, String selectQry, Object ... expected) {
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(selectQry));

        List<List<?>> results = cursor.getAll();

        assertEquals(1, results.size());

        List<?> row0 = results.get(0);
        for(int col = 0; col < expected.length; ++col)
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
