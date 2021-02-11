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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

/**
 *
 */
public class QueryJoinWithDifferentNodeFiltersTest extends AbstractIndexingCommonTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final String CACHE_NAME_2 = "cache2";

    /** */
    private static final int NODE_COUNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(CACHE_NAME)
                .setBackups(1)
                .setCacheMode(CacheMode.REPLICATED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setIndexedTypes(Integer.class, Organization.class),
            new CacheConfiguration<>(CACHE_NAME_2)
                .setNodeFilter(new TestFilter())
                .setBackups(1)
                .setCacheMode(CacheMode.PARTITIONED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setIndexedTypes(Integer.class, Person.class)
            );

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName) || getTestIgniteInstanceName(1).equals(igniteInstanceName))
            cfg.setUserAttributes(F.asMap("DATA", "true"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "snapshot", false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "snapshot", false));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSize() throws Exception {
        startGrids(NODE_COUNT);

        Ignite client = startClientGrid("client");

        client.cluster().active(true);

        IgniteCache<Object, Object> cache = client.cache(CACHE_NAME);
        IgniteCache<Object, Object> cache2 = client.cache(CACHE_NAME_2);

        int size = 100;

        for (int i = 0; i < size; i++) {
            cache.put(i, new Organization(i, "Org-" + i));
            cache2.put(i, new Person(i, i, "Person-" + i));
        }

        info(cache2.query(new SqlFieldsQuery("select * from \"cache\".Organization r, \"cache2\".Person p where p.orgId=r.orgId")).getAll().toString());
    }

    /**
     *
     */
    private static class Organization {
        /** */
        @SuppressWarnings("unused") @QuerySqlField(index = true) private int orgId;

        /** */
        @SuppressWarnings("unused") private String orgName;

        /**
         *
         */
        public Organization(int orgId, String orgName) {
            this.orgId = orgId;
            this.orgName = orgName;
        }
    }

    /**
     *
     */
    private static class Person {
        /** */
        @SuppressWarnings("unused") @QuerySqlField(index = true) private int personId;

        /** */
        @SuppressWarnings("unused") @QuerySqlField(index = true) private int orgId;

        /** */
        @SuppressWarnings("unused") private String name;

        /**
         *
         */
        public Person(int personId, int orgId, String name) {
            this.personId = personId;
            this.orgId = orgId;
            this.name = name;
        }
    }

    /**
     *
     */
    private static class TestFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return clusterNode.attribute("DATA") != null;
        }
    }
}
