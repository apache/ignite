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

import java.util.Collection;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/** Tests the scenario when a node rejoins cluster with lost knowladge of previously created dynamic schema. */
@RunWith(Parameterized.class)
public class RejoinWithLostDynamicDdlTest extends GridCommonAbstractTest {
    /** */
    private static final int SERVERS_CNT = 2;

    /** */
    private static final int LOAD_CNT = 100;

    /** */
    private boolean persistence;

    /** */
    private IgniteEx sqlClient;

    /** Grid to test (restart). */
    @Parameterized.Parameter
    public int gridToRestart;

    /** Enables create-if-not-exist table with the rejoining. */
    @Parameterized.Parameter(1)
    public boolean recreateTable;

    /** */
    @Parameterized.Parameters(name = "gridToRestart={0}, recreateTable={1}")
    public static Collection<?> runConfig() {
        // Restart coordinator, another server node and client.
        return cartesianProduct(
            F.asList(0, 1, SERVERS_CNT),
            F.asList(false, true)
        );
    }

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistence)));

        cfg.setCacheConfiguration(new CacheConfiguration<?, ?>[] {cacheConfiguration("STATIC_CACHE")});

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicSchemaInmemoryActive() throws Exception {
        testRejoinWithLostDynamicSchema(false, true, false);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicSchemaInmemoryInactive() throws Exception {
        testRejoinWithLostDynamicSchema(false, false, false);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicSchemaPersistentActive() throws Exception {
        testRejoinWithLostDynamicSchema(true, true, false);
    }

    /** */
    @Test
    public void testRejoinWithLostLostDynamicTablePersistentInactive() throws Exception {
        testRejoinWithLostDynamicSchema(true, false, false);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicSchemaPersistentActiveClear() throws Exception {
        testRejoinWithLostDynamicSchema(true, true, true);
    }

    /** */
    @Test
    public void testRejoinWithLostDynamicSchemaPersistentInactiveClear() throws Exception {
        testRejoinWithLostDynamicSchema(true, false, true);
    }

    /**
     * Tests the scenario when a node rejoins cluster with lost knowladge of previously created dynamic table over
     * a predefined cache in {@link IgniteConfiguration}.
     *
     * @param persistence Flag to test with persistence or in-memory cluster.
     * @param rejoinActive Flag to rejoin to active or inactive cluster.
     * @param clearData Flag to clear test node's persistent data before rejoining. Efficient with enabled {@code persistence}.
     */
    private void testRejoinWithLostDynamicSchema(
        boolean persistence,
        boolean rejoinActive,
        boolean clearData
    ) throws Exception {
        this.persistence = persistence;

        startGrids(SERVERS_CNT);

        sqlClient = startClientGrid(G.allGrids().size());

        if (persistence)
            grid(0).cluster().state(ClusterState.ACTIVE);

        sqlClient.createCache(cacheConfiguration("DYN_CACHE"));

        awaitPartitionMapExchange();

        sql("CREATE TABLE STATIC_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_NAME=STATIC_CACHE\"");
        sql("CREATE TABLE DYN_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_NAME=DYN_CACHE\"");

        assertEquals(0, sql("SELECT * FROM STATIC_TBL").size());
        assertEquals(0, sql("SELECT * FROM DYN_TBL").size());

        String restartConsId = grid(gridToRestart).cluster().localNode().consistentId().toString();

        stopGrid(gridToRestart);

        if (clearData)
            cleanPersistenceDir(restartConsId);

        if (!rejoinActive)
            grid(gridToRestart == SERVERS_CNT ? 1 : SERVERS_CNT).cluster().state(ClusterState.INACTIVE);

        startGrid(gridToRestart);

        if (!rejoinActive)
            grid(gridToRestart == SERVERS_CNT ? 1 : SERVERS_CNT).cluster().state(ClusterState.ACTIVE);

        sqlClient = grid(gridToRestart);

        if (recreateTable) {
            sql("CREATE TABLE IF NOT EXISTS STATIC_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_NAME=STATIC_CACHE\"");
            sql("CREATE TABLE IF NOT EXISTS DYN_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_NAME=DYN_CACHE\"");
        }

        for (int i = 0; i < LOAD_CNT; ++i) {
            sql("INSERT INTO STATIC_TBL VALUES(" + i + ", 'value_" + i + "')");
            sql("INSERT INTO DYN_TBL VALUES(" + i + ", 'value_" + i + "')");
        }

        assertEquals(LOAD_CNT, sqlClient.cache("STATIC_CACHE").size());
        assertEquals(LOAD_CNT, sqlClient.cache("DYN_CACHE").size());
        assertEquals(LOAD_CNT, sql("SELECT * FROM STATIC_TBL").size());
        assertEquals(LOAD_CNT, sql("SELECT * FROM DYN_TBL").size());
    }

    /** */
    protected List<List<?>> sql(String sql) {
        assert sqlClient != null;

        GridQueryProcessor sqlProc = sqlClient.context().query();

        return sqlProc.querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /** */
    private static CacheConfiguration<?, ?> cacheConfiguration(String cacheName) {
        return new CacheConfiguration<>(cacheName)
            .setBackups(SERVERS_CNT - 1)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
    }
}
