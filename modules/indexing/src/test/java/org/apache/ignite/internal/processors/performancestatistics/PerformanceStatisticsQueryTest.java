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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;

/** Tests query performance statistics. */
public class PerformanceStatisticsQueryTest extends AbstractPerformanceStatisticsTest {
    /** Cache entry count. */
    private static final int ENTRY_COUNT = 100;

    /** Client. */
    private static IgniteEx client;

    /** Cache. */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrids(2);

        client = startClientGrid("client");

        client.cluster().state(ACTIVE);

        cache = client.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class, Integer.class)
                    .setTableName(DEFAULT_CACHE_NAME)))
        );

        for (int i = 0; i < ENTRY_COUNT; i++)
            cache.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        String sql = "SELECT * FROM " + DEFAULT_CACHE_NAME;

        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setSchema(DEFAULT_CACHE_NAME);

        checkQuery(SQL_FIELDS, qry, sql);
    }

    /** @throws Exception If failed. */
    @Test
    public void testScanQuery() throws Exception {
        checkQuery(SCAN, new ScanQuery<>(), DEFAULT_CACHE_NAME);
    }

    /** Check query. */
    private void checkQuery(GridCacheQueryType type, Query<?> qry, String text) throws Exception {
        client.cluster().state(INACTIVE);
        client.cluster().state(ACTIVE);

        runQueryAndCheck(type, qry, text, true);

        runQueryAndCheck(type, qry, text, false);
    }

    /** Runs query and checks statistics. */
    private void runQueryAndCheck(GridCacheQueryType expType, Query<?> qry, String expText, boolean hasPhysicalReads)
        throws Exception {
        startCollectStatistics();

        cache.query(qry).getAll();

        Set<UUID> readsNodes = new HashSet<>();

        readsNodes.add(grid(0).context().localNodeId());
        readsNodes.add(grid(1).context().localNodeId());

        AtomicInteger queryCnt = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
                long logicalReads, long physicalReads) {
                assertTrue(readsNodes.remove(nodeId));
                assertEquals(expType, type);
                assertEquals(client.localNode().id(), queryNodeId);
                assertTrue(logicalReads > 0);
                assertTrue(hasPhysicalReads ? physicalReads > 0 : physicalReads == 0);
            }

            @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
                long duration, boolean success) {
                queryCnt.incrementAndGet();

                assertEquals(client.localNode().id(), nodeId);
                assertEquals(expType, type);
                assertEquals(expText, text);
                assertTrue(startTime > 0);
                assertTrue(duration >= 0);
                assertTrue(success);
            }
        });

        assertEquals(1, queryCnt.get());
    }
}
