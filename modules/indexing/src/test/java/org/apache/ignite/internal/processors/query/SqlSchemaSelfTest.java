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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Iterator;
import java.util.List;

/**
 * Tests for schemas.
 */
public class SqlSchemaSelfTest extends GridCommonAbstractTest {
    /** Person cache name. */
    private static final String CACHE_PERSON = "PersonCache";

    /** Person cache 2 name. */
    private static final String CACHE_PERSON_2 = "PersonCache2";

    /** Node. */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = (IgniteEx)startGrid();

        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test query without caches.
     *
     * @throws Exception If failed.
     */
    public void testQueryWithoutCacheOnPublicSchema() throws Exception {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT 1").setSchema("PUBLIC");

        List<List<?>> res = qryProc.querySqlFieldsNoCache(qry, true).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(1, res.get(0).get(0));

        Iterator<List<?>> iter = qryProc.querySqlFieldsNoCache(qry, true).iterator();

        assertTrue(iter.hasNext());

        List<?> row = iter.next();

        assertEquals(1, row.size());
        assertEquals(1, row.get(0));

        assertFalse(iter.hasNext());
    }

    /**
     * Test query without caches.
     *
     * @throws Exception If failed.
     */
    public void testQueryWithoutCacheOnCacheSchema() throws Exception {
        node.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON)
            .setIndexedTypes(PersonKey.class, Person.class));

        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT 1").setSchema(CACHE_PERSON);

        List<List<?>> res = qryProc.querySqlFieldsNoCache(qry, true).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(1, res.get(0).get(0));

        Iterator<List<?>> iter = qryProc.querySqlFieldsNoCache(qry, true).iterator();

        assertTrue(iter.hasNext());

        List<?> row = iter.next();

        assertEquals(1, row.size());
        assertEquals(1, row.get(0));

        assertFalse(iter.hasNext());
    }

    /**
     * Test simple query.
     *
     * @throws Exception If failed.
     */
    public void testSchemaChange() throws Exception {
        IgniteCache<PersonKey, Person> cache = node.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON)
            .setIndexedTypes(PersonKey.class, Person.class));

        node.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON_2)
            .setIndexedTypes(PersonKey.class, Person.class));

        cache.put(new PersonKey(1), new Person("Vasya", 2));

        // Normal calls.
        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM Person")
        ).getAll().size());

        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM Person").setSchema(CACHE_PERSON)
        ).getAll().size());

        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM \"PersonCache\".Person")
        ).getAll().size());

        // Call from default schema.
        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM \"PersonCache\".Person").setSchema(QueryUtils.DFLT_SCHEMA)
        ).getAll().size());

        // Call from another schema.
        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM \"PersonCache\".Person").setSchema(CACHE_PERSON_2)
        ).getAll().size());
    }

    /**
     * Test simple query.
     *
     * @throws Exception If failed.
     */
    public void testSchemaChangeOnCacheWithPublicSchema() throws Exception {
        IgniteCache<PersonKey, Person> cache = node.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON)
            .setIndexedTypes(PersonKey.class, Person.class)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA));

        node.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON_2)
            .setIndexedTypes(PersonKey.class, Person.class));

        cache.put(new PersonKey(1), new Person("Vasya", 2));

        // Normal calls.
        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM Person")
        ).getAll().size());

        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM Person").setSchema(QueryUtils.DFLT_SCHEMA)
        ).getAll().size());

        // Call from another schema.
        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM public.Person").setSchema(CACHE_PERSON_2)
        ).getAll().size());

        assertEquals(1, cache.query(
            new SqlFieldsQuery("SELECT id, name, orgId FROM \"PUBLIC\".Person").setSchema(CACHE_PERSON_2)
        ).getAll().size());
    }

    /**
     * Test type conflict in public schema.
     *
     * @throws Exception If failed.
     */
    public void _testTypeConflictInPublicSchema() throws Exception {
        // TODO: IGNITE-5380: uncomment work after fix.
        fail("Hang for now, need to fix");

        node.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON)
            .setIndexedTypes(PersonKey.class, Person.class)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA));

        node.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON_2)
            .setIndexedTypes(PersonKey.class, Person.class)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA));
    }

    /**
     * Person key.
     */
    public static class PersonKey {
        @QuerySqlField
        public long id;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        PersonKey(long id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj != null && obj instanceof PersonKey && (F.eq(id, ((PersonKey)obj).id));
        }
    }

    /**
     * Person.
     */
    public static class Person {
        /** Name. */
        @QuerySqlField
        public String name;

        /** Organization ID. */
        @QuerySqlField(index = true)
        public long orgId;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param orgId Orgainzation ID.
         */
        public Person(String name, long orgId) {
            this.name = name;
            this.orgId = orgId;
        }
    }

}
