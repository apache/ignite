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

import java.util.Arrays;
import java.util.LinkedHashMap;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests cache in-place modification logic with iterative value increment.
 */
@Ignore("https://issues.apache.org/jira/browse/IGNITE-2229")
public class IgniteCacheFullTextQueryNodeJoiningSelfTest extends GridCommonAbstractTest {
    /** Number of nodes to test on. */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cache = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cache.setCacheMode(PARTITIONED);
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setBackups(1);
        cache.setRebalanceMode(CacheRebalanceMode.SYNC);

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(AffinityKey.class.getName());
        qryEntity.setValueType(IndexedEntity.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("val", String.class.getName());

        qryEntity.setFields(fields);

        qryEntity.setIndexes(Arrays.asList(new QueryIndex("val", QueryIndexType.FULLTEXT)));

        cache.setQueryEntities(Arrays.asList(qryEntity));

        cfg.setCacheConfiguration(cache);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFullTextQueryNodeJoin() throws Exception {
        for (int r = 0; r < 5; r++) {
            startGrids(GRID_CNT);

            try {
                for (int i = 0; i < 1000; i++) {
                    IndexedEntity entity = new IndexedEntity("indexed " + i);

                    grid(0).cache(DEFAULT_CACHE_NAME).put(new AffinityKey<>(i, i), entity);
                }

                Ignite started = startGrid(GRID_CNT);

                for (int i = 0; i < 100; i++) {
                    QueryCursor<Cache.Entry<AffinityKey<Integer>, IndexedEntity>> res = started.cache(DEFAULT_CACHE_NAME)
                        .query(new TextQuery<AffinityKey<Integer>, IndexedEntity>(IndexedEntity.class, "indexed"));

                    assertEquals("Failed iteration: " + i, 1000, res.getAll().size());
                }
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /** */
    private static class IndexedEntity {
        /** */
        private String val;

        /**
         * @param val Value.
         */
        private IndexedEntity(String val) {
            this.val = val;
        }
    }
}
