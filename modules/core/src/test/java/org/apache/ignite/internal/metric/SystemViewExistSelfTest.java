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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.IgniteComponentType.INDEXING;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_METRICS_SYS_VIEW;
import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_IO_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.PART_STATES_VIEW;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.BINARY_METADATA_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.METASTORE_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.DATA_REGION_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODES_SYS_VIEW;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LATCHES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LOCKS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LONGS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.QUEUES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.REFERENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEMAPHORES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEQUENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SETS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.STAMPED_VIEW;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl.DISTRIBUTED_METASTORE_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;

/** Tests for {@link SystemView}. */
@RunWith(Parameterized.class)
public class SystemViewExistSelfTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameters(name = "isPersistence={0}")
    public static Iterable<Boolean[]> data() {
        return Arrays.asList(new Boolean[] {true}, new Boolean[] {false});
    }

    /** */
    @Parameterized.Parameter
    public boolean isPersistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setName("pds")
                    .setPersistenceEnabled(isPersistence)));
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
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testViews() throws Exception {
        Set<String> expViews = new HashSet<>(Arrays.asList(
            BASELINE_NODE_ATTRIBUTES_SYS_VIEW,
            BASELINE_NODES_SYS_VIEW,
            BINARY_METADATA_VIEW,
            CACHE_GRP_PAGE_LIST_VIEW,
            CACHE_GRPS_VIEW,
            CACHES_VIEW,
            CLI_CONN_VIEW,
            CQ_SYS_VIEW,
            DATA_REGION_PAGE_LIST_VIEW,
            STREAM_POOL_QUEUE_VIEW,
            DISTRIBUTED_METASTORE_VIEW,
            JOBS_VIEW,
            CACHE_GRP_IO_VIEW,
            NODE_ATTRIBUTES_SYS_VIEW,
            NODE_METRICS_SYS_VIEW,
            NODES_SYS_VIEW,
            PART_STATES_VIEW,
            SCAN_QRY_SYS_VIEW,
            SVCS_VIEW,
            SYS_POOL_QUEUE_VIEW,
            TASKS_VIEW,
            TXS_MON_LIST,
            QUEUES_VIEW,
            SETS_VIEW,
            LOCKS_VIEW,
            SEMAPHORES_VIEW,
            LATCHES_VIEW,
            LATCHES_VIEW,
            STAMPED_VIEW,
            REFERENCES_VIEW,
            LONGS_VIEW,
            SEQUENCES_VIEW
        ));

        if (INDEXING.inClassPath()) {
            expViews.addAll(Arrays.asList(
                "sql.queries",
                "statistics.configuration",
                "statisticsPartitionData",
                "tables",
                "indexes",
                "views",
                "view.columns",
                "sql.queries.history",
                "statisticsLocalData",
                "table.columns",
                "schemas")
            );
        }

        if (isPersistence)
            expViews.add(METASTORE_VIEW);

        IgniteEx ignite = startGrid();

        if (isPersistence) {
            assertEquals(ClusterState.INACTIVE, ignite.cluster().state());

            assertEquals(expViews, getAllViewNames(ignite));
        }

        ignite.cluster().state(ClusterState.ACTIVE);

        assertEquals(expViews, getAllViewNames(ignite));

        if (isPersistence) {
            ignite.cluster().state(ClusterState.INACTIVE);

            assertEquals(expViews, getAllViewNames(ignite));
        }
    }

    /**
     * @param ignite Ignite.
     */
    private Set<String> getAllViewNames(IgniteEx ignite) {
        GridSystemViewManager views = ignite.context().systemView();

        Iterator<SystemView<?>> iter = views.iterator();

        Set<String> actualViews = new HashSet<>();

        while (iter.hasNext())
            actualViews.add(iter.next().name());

        return actualViews;
    }
}
