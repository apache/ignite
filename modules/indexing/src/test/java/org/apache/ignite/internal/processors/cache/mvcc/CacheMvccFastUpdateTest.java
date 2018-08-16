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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.io.Serializable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 *
 */
public class CacheMvccFastUpdateTest extends CacheMvccAbstractTest {
    /**
     *
     */
    public static class Person implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private Integer id;

        /** */
        @QuerySqlField
        private String name;

        /** */
        public Person() {
        }

        /** */
        public Person(Integer id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    /** */
    private IgniteCache<?, ?> cache;

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cache = startGrid(0).getOrCreateCache(
            new CacheConfiguration<>("test")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setSqlSchema("PUBLIC")
                .setIndexedTypes(Integer.class, Person.class)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testFastUpdateShouldReportZeroIfNoEntriesWereUpdated() throws Exception {
        Object res = cache.query(new SqlFieldsQuery("update Person set _val = 'a' where _key = 1"))
            .getAll().get(0).get(0);

        assertEquals("0", res.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFastUpdateShouldReportOneIfEntryWasUpdated() throws Exception {
        cache.query(new SqlFieldsQuery("insert into Person(_key, _val) values(1, ?)").setArgs(new Person(1, "b")));

        Object res = cache.query(new SqlFieldsQuery("update Person set _val = 'a' where _key = 1"))
            .getAll().get(0).get(0);

        assertEquals("1", res.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFastDeleteShouldReportZeroIfNoEntriesWereUpdated() throws Exception {
        Object res = cache.query(new SqlFieldsQuery("delete from Person where _key = 1"))
            .getAll().get(0).get(0);

        assertEquals("0", res.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFastDeleteShouldReportOneIfEntryWasUpdated() throws Exception {
        cache.query(new SqlFieldsQuery("insert into Person(_key, _val) values(1, ?)").setArgs(new Person(1, "b")));

        Object res = cache.query(new SqlFieldsQuery("delete from Person where _key = 1"))
            .getAll().get(0).get(0);

        assertEquals("1", res.toString());
    }
}
