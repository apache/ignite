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

import java.io.Serializable;
import java.sql.PreparedStatement;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class SqlFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** INSERT statement. */
    private final static String INSERT = "insert into Person(_key, name) values (5, 'x')";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If error.
     */
    public void testSqlFieldsQuery() throws Exception {
        startGrids(1);

        createAndFillCache();

        executeQuery();
    }

    /**
     * @throws Exception If error.
     */
    public void testSqlFieldsQueryWithTopologyChanges() throws Exception {
        startGrid(0);

        createAndFillCache();

        startGrid(1);

        executeQuery();
    }

    /**
     * @throws Exception If error.
     */
    public void testQueryCaching() throws Exception {
        startGrid(0);

        PreparedStatement stmt = null;

        for (int i = 0; i < 2; i++) {
            createAndFillCache();

            PreparedStatement stmt0 = grid(0).context().query().prepareNativeStatement("person", INSERT);

            // Statement should either be parsed initially or in response to schema change...
            assertTrue(stmt != stmt0);

            stmt = stmt0;

            // ...and be properly compiled considering schema changes to be properly parsed
            new GridSqlQueryParser(false).parse(GridSqlQueryParser.prepared(stmt));

            destroyCache();
        }

        stmt = null;

        createAndFillCache();

        // Now let's do the same without restarting the cache.
        for (int i = 0; i < 2; i++) {
            PreparedStatement stmt0 = grid(0).context().query().prepareNativeStatement("person", INSERT);

            // Statement should either be parsed or taken from cache as no schema changes occurred...
            assertTrue(stmt == null || stmt == stmt0);

            stmt = stmt0;

            // ...and be properly compiled considering schema changes to be properly parsed
            new GridSqlQueryParser(false).parse(GridSqlQueryParser.prepared(stmt));
        }

        destroyCache();
    }

    /**
     *
     */
    private void executeQuery() throws IgniteCheckedException {
        IgniteCache<Integer, Object> cache = grid(0).cache("person");

        int cnt = H2Tree.cnt.get();

        for (int i = 2; i <= 2; i++) {
            cache.put(i, new Person("sun", i * 10));

            int nextCnt = H2Tree.cnt.get();

            System.out.println("Comparisons to put " + i + ": " + (nextCnt - cnt));
            System.out.println("Size: " + H2Tree.instance.size());

            cnt = nextCnt;
        }

        for (int i = 2; i <= 2; i++) {
            cache.put(i, new Person("sun", i * 10));

            int nextCnt = H2Tree.cnt.get();

            System.out.println("Comparisons to put " + i + ": " + (nextCnt - cnt));
            System.out.println("Size: " + H2Tree.instance.size());

            cnt = nextCnt;
        }

        for (int i = 2; i <= 2; i++) {
            cache.put(i, new Person("sun", i * 10));

            int nextCnt = H2Tree.cnt.get();

            System.out.println("Comparisons to put " + i + ": " + (nextCnt - cnt));
            System.out.println("Size: " + H2Tree.instance.size());

            cnt = nextCnt;
        }
    }


    /**
     *
     */
    private IgniteCache<Integer, Person> createAndFillCache() throws IgniteCheckedException {
        CacheConfiguration<Integer, Person> cacheConf = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheConf.setCacheMode(CacheMode.PARTITIONED);
        cacheConf.setBackups(0);

        cacheConf.setIndexedTypes(Integer.class, Person.class);

        cacheConf.setName("person");

        IgniteCache<Integer, Person> cache = grid(0).createCache(cacheConf);

        int cnt = H2Tree.cnt.get();

        for (int i = 1; i <= 2; i++) {
            cache.put(i, new Person("sun", i * 10));

            int nextCnt = H2Tree.cnt.get();

            System.out.println("Comparisons to put " + i + ": " + (nextCnt - cnt));
            System.out.println("Size: " + H2Tree.instance.size());

            cnt = nextCnt;
        }

        return cache;
    }

    private void destroyCache() {
        grid(0).destroyCache("person");
    }

    /**
     *
     */
    public static class Person implements Serializable {
        /** Name. */
        @QuerySqlField
        private String name;

        /** Age. */
        @QuerySqlField(index = true)
        private int age;

        /**
         * @param name Name.
         * @param age Age.
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /**
         *
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         *
         */
        public int getAge() {
            return age;
        }

        /**
         * @param age Age.
         */
        public void setAge(int age) {
            this.age = age;
        }
    }
}
