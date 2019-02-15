/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests non-collocated join with REPLICATED cache and no primary partitions for that cache on some nodes.
 */
@SuppressWarnings("unused")
@RunWith(JUnit4.class)
public class IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest extends GridCommonAbstractTest {
    /** Client node name. */
    public static final String NODE_CLI = "client";

    /** */
    public static final String CACHE_PARTITIONED = "partitioned";

    /** */
    public static final String CACHE_REPLICATED = "replicated";

    /** */
    public static final int REP_CNT = 3;

    /** */
    public static final int PART_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(F.eq(NODE_CLI, igniteInstanceName));

        CacheConfiguration<Integer, PartValue> ccfg1 = new CacheConfiguration<>(CACHE_PARTITIONED);

        ccfg1.setCacheMode(PARTITIONED);
        ccfg1.setAtomicityMode(TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(FULL_SYNC);
        ccfg1.setIndexedTypes(Integer.class, PartValue.class);
        ccfg1.setSqlSchema(QueryUtils.DFLT_SCHEMA);

        CacheConfiguration<Integer, RepValue> ccfg2 = new CacheConfiguration<>(CACHE_REPLICATED);

        ccfg2.setAffinity(new RendezvousAffinityFunction(false, REP_CNT));
        ccfg2.setCacheMode(REPLICATED);
        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(FULL_SYNC);
        ccfg2.setIndexedTypes(Integer.class, RepValue.class);
        ccfg2.setSqlSchema(QueryUtils.DFLT_SCHEMA);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /**{@inheritDoc}  */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(3);

        Ignite cli = startGrid(NODE_CLI);

        for (int i = 0; i < REP_CNT; i++)
            cli.cache(CACHE_REPLICATED).put(i, new RepValue(i));

        for (int i = 0; i < PART_CNT; i++)
            cli.cache(CACHE_PARTITIONED).put(i, new PartValue(i, ((i + 1) % REP_CNT)));
    }

    /**{@inheritDoc}  */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test non-colocated join.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJoinNonCollocated() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT COUNT(*) FROM PartValue p, RepValue r WHERE p.repId=r.id");

        long cnt = (Long)grid(NODE_CLI).cache(CACHE_PARTITIONED).query(qry).getAll().get(0).get(0);

        assertEquals(PART_CNT, cnt);
    }

    /**
     * Value for PARTITIONED cache.
     */
    public static class PartValue {
        /** Id. */
        @QuerySqlField
        private int id;

        /** Rep id. */
        @QuerySqlField
        private int repId;

        /**
         * @param id Id.
         * @param repId Rep id.
         */
        public PartValue(int id, int repId) {
            this.id = id;
            this.repId = repId;
        }
    }

    /**
     * Value for REPLICATED cache.
     */
    public static class RepValue {
        /** Id. */
        @QuerySqlField
        private int id;

        /**
         * @param id Id.
         */
        public RepValue(int id) {
            this.id = id;
        }
    }
}
