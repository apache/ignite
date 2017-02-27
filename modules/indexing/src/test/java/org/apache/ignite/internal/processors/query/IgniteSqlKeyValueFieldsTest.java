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
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Test hidden _key, _val, _ver columns
 */
public class IgniteSqlKeyValueFieldsTest  extends GridCommonAbstractTest {

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name of the cache for test */
    private static final String CACHE_NAME = "keyvaltest";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new BinaryMarshaller());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    private CacheConfiguration buildPrimitiveCacheConfig(boolean setKeyValNames, boolean setFields) {
        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Integer.class.getName());

        if (setKeyValNames) {
            entity.setKeyFieldName("id");
            entity.setValueFieldName("v");
            entity.setVersionFieldName("ver");
        }

        if (setFields) {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("v", Integer.class.getName());
            fields.put("id", Integer.class.getName());

            entity.setFields(fields);
        }

        ccfg.setQueryEntities(Arrays.asList(entity));
        return ccfg;
    }

    /** */
    private CacheConfiguration buildPersonCacheConfig(boolean setKeyValNames, boolean setFields) {
        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        if (setKeyValNames) {
            entity.setKeyFieldName("id");
            entity.setValueFieldName("v");
            entity.setVersionFieldName("ver");
        }

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("name", String.class.getName());
        fields.put("age", Integer.class.getName());

        if (setFields) {
            fields.put(entity.getKeyFieldName(), entity.getKeyType());
            fields.put(entity.getValueFieldName(), entity.getValueType());
        }

        entity.setFields(fields);

        ccfg.setQueryEntities(Arrays.asList(entity));

        return ccfg;
    }

    /** */
    public void testPrimitiveNoKeyValueAliases() throws Exception {
        //_ver|_key,_val
        CacheConfiguration ccfg = buildPrimitiveCacheConfig(false, false);

        try {
            IgniteCache<Integer, Integer> cache = ignite(0).getOrCreateCache(ccfg);

            checkInsert(cache, "insert into Integer (_key, _val) values (?,?)", 1, 100);

            checkSelect(cache, "select * from Integer", 1, 100);
            checkSelect(cache, "select _key, _val from Integer", 1, 100);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /** */
    public void testKeyValueAliasesNoFieldsSetup() throws Exception {
        //_ver|id(_key), v(_val)
        CacheConfiguration ccfg = buildPrimitiveCacheConfig(true, false);

        try {
            IgniteCache<Integer, Integer> cache = ignite(0).getOrCreateCache(ccfg);

            checkInsert(cache, "insert into Integer (_key, _val) values (?,?)", 1, 100);
            checkInsert(cache, "insert into Integer (id, v) values (?,?)", 2, 200);

            checkSelect(cache, "select * from Integer where _key=1", 1, 100);
            checkSelect(cache, "select _key, _val from Integer where id=1", 1, 100);

            checkSelect(cache, "select * from Integer where _key=2", 2, 200);
            checkSelect(cache, "select _key, _val from Integer where id=2", 2, 200);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /** */
    public void testPrimitiveKeyValueAliasesFieldsGiven() throws Exception {
        //_ver|v(_val), id(_key) - order given by fields
        CacheConfiguration ccfg = buildPrimitiveCacheConfig(true, true);

        try {
            IgniteCache<Integer, Integer> cache = ignite(0).getOrCreateCache(ccfg);

            checkInsert(cache, "insert into Integer (_key, _val) values (?,?)", 1, 100);
            checkInsert(cache, "insert into Integer (id, v) values (?,?)", 2, 200);

            checkSelect(cache, "select * from Integer where _key=1", 100, 1);
            checkSelect(cache, "select _key, _val from Integer where id=1", 1, 100);

            checkSelect(cache, "select * from Integer where _key=2", 200, 2);
            checkSelect(cache, "select _key, _val from Integer where id=2", 2, 200);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /** */
    public void testNoKeyValueAliases() throws Exception {
        //_key,_val,_ver | name, age
        CacheConfiguration ccfg = buildPersonCacheConfig(false, false);

        Person alice = new Person("Alice", 1);

        try {
            IgniteCache<Integer, Person> cache = ignite(0).getOrCreateCache(ccfg);

            checkInsert(cache, "insert into Person (_key, _val) values (?,?)", 1, alice);

            checkSelect(cache, "select * from Person", alice.name, alice.age);
            checkSelect(cache, "select _key, _val from Person", 1, alice);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /** */
    public void testKeyValueAliasesHiddenFields() throws Exception {
        //id(_key),v(_val),_ver | name, age
        CacheConfiguration ccfg = buildPersonCacheConfig(true, false);

        Person alice = new Person("Alice", 1);
        Person bob = new Person("Bob", 2);

        try {
            IgniteCache<Integer, Person> cache = ignite(0).getOrCreateCache(ccfg);

            checkInsert(cache, "insert into Person (_key, _val) values (?,?)", 1, alice);
            checkInsert(cache, "insert into Person (id, v) values (?,?)", 2, bob);

            checkSelect(cache, "select * from Person where _key=1", alice.name, alice.age);
            checkSelect(cache, "select _key, _val from Person where id=1", 1, alice);

            checkSelect(cache, "select * from Person where _key=2", bob.name, bob.age);
            checkSelect(cache, "select _key, _val from Person where id=2", 2, bob);

        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /** */
    public void testKeyValueAliasesVisibleFields() throws Exception {
        //_ver | name, age, id(_key), v(_val)

        CacheConfiguration ccfg = buildPersonCacheConfig(true, true);
        Person alice = new Person("Alice", 1);
        Person bob = new Person("Bob", 2);

        try {
            IgniteCache<Integer, Person> cache = ignite(0).getOrCreateCache(ccfg);

            checkInsert(cache, "insert into Person (_key, _val) values (?,?)", 1, alice);
            checkInsert(cache, "insert into Person (id, v) values (?,?)", 2, bob);

            checkSelect(cache, "select * from Person where _key=1", alice.name, alice.age, 1, alice);
            checkSelect(cache, "select _key, _val from Person where id=1", 1, alice);

            checkSelect(cache, "select * from Person where _key=2", bob.name, bob.age, 2, bob);
            checkSelect(cache, "select _key, _val from Person where id=2", 2, bob);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /** */
    public void testVersionField() throws Exception {
        //_ver | name, age, id(_key), v(_val)

        CacheConfiguration ccfg = buildPersonCacheConfig(true, true);
        Person alice = new Person("Alice", 1);
        Person bob = new Person("Bob", 2);

        try {
            IgniteCache<Integer, Person> cache = ignite(0).getOrCreateCache(ccfg);

            checkInsert(cache, "insert into Person (id, v) values (?,?)", 1, alice);

            checkInsert(cache, "insert into Person (id, v) values (?,?)", 2, bob);

            byte[] v1 = getVersion(cache, 1);

            checkInsert(cache, "update Person set age = ? where id = ?", 3, 1);

            byte[] v2 = getVersion(cache, 1);

            assertFalse( Arrays.equals(v1, v2) );
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /** */
    private byte[] getVersion(IgniteCache<?, ?> cache, int key) {
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select _ver from Person where id = ?").setArgs(key));
        List<List<?>> results = cursor.getAll();
        assertEquals(1, results.size());
        return ((byte[])results.get(0).get(0));
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
            assertEquals(row0.get(col), expected[col]);
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
