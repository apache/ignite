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

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests for correct distributed sql joins.
 */
public class IgniteSqlDistributedJoinSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private static final int ORG_COUNT = NODES_COUNT;

    /** */
    private static final int PERSON_PER_ORG_COUNT = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_COUNT, false);
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setIndexedTypes(idxTypes);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonCollocatedDistributedJoin() throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", true, String.class, Person.class);
        CacheConfiguration ccfg2 = cacheConfig("org", true, String.class, Organization.class);

        IgniteCache<String, Person> c1 = ignite(0).getOrCreateCache(ccfg1);
        IgniteCache<String, Organization> c2 = ignite(0).getOrCreateCache(ccfg2);

        try {
            awaitPartitionMapExchange();

            populateDataIntoCaches(c1, c2);

            String joinSql =
                "select * from Person, \"org\".Organization as org " +
                    "where Person.orgId = org.id " +
                    "and lower(org.name) = lower(?)";

            SqlQuery qry = new SqlQuery<String, Person>(Person.class, joinSql).setArgs("Organization #0");

            qry.setDistributedJoins(true);

            List<Person> prns = c1.query(qry).getAll();

            assertEquals(PERSON_PER_ORG_COUNT, prns.size());
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @param c1 Cache1.
     * @param c2 Cache2.
     */
    private void populateDataIntoCaches(IgniteCache<String, Person> c1, IgniteCache<String, Organization> c2) {
        int personId = 0;

        for (int i = 0; i < ORG_COUNT; i++) {
            Organization org = new Organization();
            org.setId("org" + i);
            org.setName("Organization #" + i);

            c2.put(org.getId(), org);

            for (int j = 0; j < PERSON_PER_ORG_COUNT; j++) {
                Person prsn = new Person();
                prsn.setId("pers" + personId);
                prsn.setOrgId(org.getId());
                prsn.setName("Person name #" + personId);

                c1.put(prsn.getId(), prsn);

                personId++;
            }
        }
    }

    /**
     *
     */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String orgId;

        /** */
        @QuerySqlField(index = true)
        private String name;

        public String getId() { return id; }

        public void setId(String id) { this.id = id; }

        public String getOrgId() { return orgId; }

        public void setOrgId(String orgId) { this.orgId = orgId; }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }
    }

    /**
     *
     */
    private static class Organization {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String name;

        public void setId(String id) { this.id = id; }

        public String getId() { return id; }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }
    }
}
