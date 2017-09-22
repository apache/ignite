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

package org.apache.ignite.java8.examples;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test covering bridge methods changes in Java 8.
 */
public class IndexingBridgeMethodTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testBridgeMethod() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<Integer, MyType> ccfg = new CacheConfiguration<>();

        ccfg.setName("mytype");
        ccfg.setIndexedTypes(Integer.class, MyType.class);

        IgniteCache<Integer,MyType> c = ignite.getOrCreateCache(ccfg);

        for (int i = 0; i < 100; i++)
            c.put(i, new MyType(i));

        assertEquals(100L, c.query(new SqlFieldsQuery(
            "select count(*) from MyType")).getAll().get(0).get(0));
        assertEquals(15, c.query(new SqlFieldsQuery(
            "select id from MyType where _key = 15")).getAll().get(0).get(0));
        assertEquals(25, c.query(new SqlFieldsQuery(
            "select _key from MyType where id = 25")).getAll().get(0).get(0));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Classes implementing this method, will have bridge method.
     */
    private static interface HasId<T extends Number> {
        /**
         * @return ID.
         */
        public T getId();
    }

    /**
     *
     */
    private static class MyType implements HasId<Integer> {
        /** */
        private int id;

        /**
         * @param id Id.
         */
        private MyType(int id) {
            this.id = id;
        }

        /**
         * @return ID.
         */
        @QuerySqlField(index = true)
        @Override public Integer getId() {
            return id;
        }
    }
}
