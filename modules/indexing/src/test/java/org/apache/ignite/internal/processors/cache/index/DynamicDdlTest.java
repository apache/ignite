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

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class DynamicDdlTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /** Server cache configurations. */
    private CacheConfiguration<?, ?>[] predefinedCachesCfgs;

    /** */
    private IgniteEx sqlClient;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistence)));

        if (!F.isEmpty(predefinedCachesCfgs))
            cfg.setCacheConfiguration(predefinedCachesCfgs);

        return cfg;
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicTableOverPredefinedCacheInmemoryActive() throws Exception {
        testRejoinWithLostDynamicTableOverPredefinedCache(false, true, false);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicTableOverPredefinedCacheInmemoryInactive() throws Exception {
        testRejoinWithLostDynamicTableOverPredefinedCache(false, false, false);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicTableOverPredefinedCachePersistentActive() throws Exception {
        testRejoinWithLostDynamicTableOverPredefinedCache(true, true, false);
    }

    /** */
    @Test
    public void testRejoinWithLostLostDynamicTableOverPredefinedCachePersistentInactive() throws Exception {
        testRejoinWithLostDynamicTableOverPredefinedCache(true, false, false);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicTableOverPredefinedCachePersistentActiveClear() throws Exception {
        testRejoinWithLostDynamicTableOverPredefinedCache(true, true, true);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicTableOverPredefinedCachePersistentInactiveClear() throws Exception {
        testRejoinWithLostDynamicTableOverPredefinedCache(true, false, true);
    }

    /**
     * Tests the scenario when a node rejoins cluster with lost knowladge of previously dynamically created table over
     * a predefined in {@link IgniteConfiguration} cache.
     *
     * @param persistence Flag to test with persistence or in-memory cluster.
     * @param active Flag to rejoin to active or inactive cluster.
     * @param clearData Flag to clear test node's persistent data before rejoining. Efficient with enabled {@code persistence}.
     */
    private void testRejoinWithLostDynamicTableOverPredefinedCache(
        boolean persistence,
        boolean active,
        boolean clearData
    ) throws Exception {
        this.persistence = persistence;

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("TEST_CACHE")
            .setBackups(1)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        predefinedCachesCfgs = new CacheConfiguration<?, ?>[] {cacheCfg};

        startGrids(3);

        sqlClient = startClientGrid(G.allGrids().size());

        sqlClient.cluster().state(ClusterState.ACTIVE);

        sql("CREATE TABLE IF NOT EXISTS TEST_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_NAME=TEST_CACHE\"");

        assertEquals(0, sql("SELECT * FROM TEST_TBL").size());

        // Grid to restart.
        int testGrid = 1;

        String testConsId = grid(testGrid).cluster().localNode().toString();

        stopGrid(testGrid);

        if (clearData)
            cleanPersistenceDir(testConsId);

        if (!active)
            sqlClient.cluster().state(ClusterState.INACTIVE);

        startGrid(testGrid);

        if (!active)
            sqlClient.cluster().state(ClusterState.ACTIVE);

        // Inserts work even the table might be lost.
        for (int i = 0; i < 100; ++i)
            assertEquals(1, sql("INSERT INTO TEST_TBL VALUES(" + (i + 1) + ", '" + (i + 1000) + "')").size());

        assertEquals(100, grid(testGrid).cache("TEST_CACHE").size());

        assertEquals(100, sql("SELECT * FROM TEST_TBL").size());
    }

    /** */
    protected List<List<?>> sql(String sql) {
        assert sqlClient != null;

        GridQueryProcessor sqlProc = sqlClient.context().query();

        return sqlProc.querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }
}
