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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Based scanCount with offheap index issue.
 */
public class IgniteCacheOffheapIndexScanTest extends GridCommonAbstractTest {
    /** */
    private static IgniteCache<Integer, Object> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(LOCAL);
        cacheCfg.setIndexedTypes(
            Integer.class, Person.class
        );

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(1, false);

        cache = grid(0).cache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cache = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryPlan() throws Exception {
        for (int i = 0; i < 1000; i++)
            cache.put(i, new Person(i, "firstName" + i, "lastName" + i, i % 100));

        final AtomicBoolean end = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!end.get())
                    cache.query(new SqlFieldsQuery("select _val from Person")).getAll();

                return null;
            }
        }, 5);

        for (int i = 0; i < 150; i++) {
            String plan = (String)cache.query(new SqlFieldsQuery(
                "explain analyze select count(*) from Person where salary = 50")).getAll().get(0).get(0);

            assertTrue(plan, plan.contains("scanCount: 11 "));

            Thread.sleep(100);
        }

        end.set(true);

        fut.get();
    }

    /**
     * Person record used for query test.
     */
    public static class Person implements Serializable {
        /** Person ID. */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary. */
        @QuerySqlField(index = true)
        private double salary;

        /**
         * Constructs empty person.
         */
        public Person() {
            // No-op.
        }

        /**
         * Constructs person record that is not linked to any organization.
         *
         * @param id Person ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        public Person(int id, String firstName, String lastName, double salary) {
            this(id, 0, firstName, lastName, salary);
        }

        /**
         * Constructs person record.
         *
         * @param id Person ID.
         * @param orgId Organization ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        public Person(int id, int orgId, String firstName, String lastName, double salary) {
            this.id = id;
            this.orgId = orgId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || (o instanceof Person) && id == ((Person)o).id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", id=" + id +
                ", orgId=" + orgId +
                ", lastName=" + lastName +
                ", salary=" + salary +
                ']';
        }
    }
}
