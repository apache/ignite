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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests non collocated join with replicated cache.
 */
public class IgniteCacheReplicatedJoinSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String REP_CACHE_NAME = "repCache";

    /** */
    public static final String PART_CACHE_NAME = "partCache";

    /** */
    public static final int REP_CNT = 3;

    /** */
    public static final int PART_CNT = 10_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        final CacheConfiguration<Integer, PartValue> ccfg1 = new CacheConfiguration<>(PART_CACHE_NAME);
        ccfg1.setCacheMode(PARTITIONED);
        ccfg1.setAtomicityMode(TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(FULL_SYNC);
        ccfg1.setIndexedTypes(Integer.class, PartValue.class);

        final CacheConfiguration<Integer, RepValue> ccfg2 = new CacheConfiguration<>(REP_CACHE_NAME);
        ccfg2.setAffinity(new RendezvousAffinityFunction(false, REP_CNT));
        ccfg2.setCacheMode(REPLICATED);
        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(FULL_SYNC);
        ccfg2.setIndexedTypes(Integer.class, RepValue.class);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /**
     * Test non-colocated join.
     *
     * @throws Exception If failed.
     */
    public void testJoinNonCollocated() throws Exception {
        startGridsMultiThreaded(3);

        final Ignite client = startGrid("client");

        for (int i = 0; i < REP_CNT; i++)
            client.cache(REP_CACHE_NAME).put(i, new RepValue(i, "rep" + i));

        for (int i = 0; i < PART_CNT; i++)
            client.cache(PART_CACHE_NAME).put(i, new PartValue(i, "part" + i, ((i + 1) % REP_CNT)));

        final FieldsQueryCursor<List<?>> qry = client.cache(PART_CACHE_NAME).
            query(new SqlFieldsQuery("select PartValue._VAL, r._VAL from PartValue, \"repCache\".RepValue as r where PartValue.repId=r.id"));

        final List<List<?>> all = qry.getAll();

        assertEquals(10_000, all.size());

        for (List<?> objects : all) {
            final PartValue pv = (PartValue)objects.get(0);
            final RepValue rv = (RepValue)objects.get(1);

            assertNotNull(rv);

            assertEquals(rv.getId(), pv.getRepId());
        }
    }

    /** */
    public static class PartValue {
        /** Id. */
        @QuerySqlField
        private int id;

        /** Name. */
        @QuerySqlField
        private String name;

        /** Rep id. */
        @QuerySqlField
        private int repId;

        /**
         * @param id Id.
         * @param name Name.
         * @param repId Rep id.
         */
        public PartValue(int id, String name, int repId) {
            this.id = id;
            this.name = name;
            this.repId = repId;
        }

        /** */
        public int getId() {
            return id;
        }

        /** */
        public void setId(int id) {
            this.id = id;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }

        /** */
        public int getRepId() {
            return repId;
        }

        /** */
        public void setRepId(int repId) {
            this.repId = repId;
        }
    }

    /** */
    public static class RepValue {
        /** Id. */
        @QuerySqlField
        private int id;

        /** Name. */
        @QuerySqlField
        private String name;

        /**
         * @param id Id.
         * @param name Name.
         */
        public RepValue(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** */
        public int getId() {
            return id;
        }

        /** */
        public void setId(int id) {
            this.id = id;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }
    }
}