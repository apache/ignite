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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;

/**
 * For testing of rebalance statistics.
 * todo: need refactor
 */
@SystemPropertiesList(value = {
    @WithSystemProperty(key = IGNITE_QUIET, value = "false"),
})
public class RebalanceStatisticsTest extends GridCommonAbstractTest {
    /** Logger for listen messages. */
    private final ListeningTestLogger listenLog = new ListeningTestLogger(false, log);

    /** Caches configurations. */
    private CacheConfiguration[] cacheCfgs;

    /** Data storage configuration. */
    private DataStorageConfiguration dsCfg;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        listenLog.clearListeners();

        stopAllGrids();

        if (nonNull(dsCfg))
            cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(cacheCfgs)
            .setRebalanceThreadPoolSize(5)
            .setGridLogger(listenLog)
            .setDataStorageConfiguration(dsCfg)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /**
     * Test statistics of a full rebalance.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    public void testFullRebalanceStatistics() throws Exception {
        createCluster(3);
    }

    /**
     * Test statistics of a historical rebalance.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void testHistRebalanceStatistics() throws Exception {
        dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(200 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            ).setWalMode(WALMode.LOG_ONLY);

        IgniteEx crd = createCluster(3);
    }

    /**
     * Test checks situation when rebalance is restarted for cache group,
     * then 2 statistics will be printed for it.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    public void testBreakRebalanceChain() throws Exception {
        String filteredNodePostfix = "_filtered";

        IgnitePredicate<ClusterNode> nodeFilter =
            clusterNode -> !clusterNode.consistentId().toString().contains(filteredNodePostfix);

        cacheCfgs = new CacheConfiguration[] {
            cacheConfiguration(DEFAULT_CACHE_NAME + 1, null, 15, 1)
                .setRebalanceOrder(1)
                .setNodeFilter(nodeFilter),
            cacheConfiguration(DEFAULT_CACHE_NAME + 2, null, 15, 1)
                .setRebalanceOrder(2),
            cacheConfiguration(DEFAULT_CACHE_NAME + 3, null, 15, 1)
                .setRebalanceOrder(3)
                .setNodeFilter(nodeFilter)
        };

        int nodeCnt = 2;

        startGrids(nodeCnt);
        awaitPartitionMapExchange();

        IgniteConfiguration cfg2 = getConfiguration(getTestIgniteInstanceName(nodeCnt++));
        TestRecordingCommunicationSpi spi2 = (TestRecordingCommunicationSpi)cfg2.getCommunicationSpi();

        int restartRebalanceCacheId = cacheId(DEFAULT_CACHE_NAME + 1);

        spi2.blockMessages((clusterNode, msg) -> {
            if (GridDhtPartitionDemandMessage.class.isInstance(msg)) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                if (demandMsg.groupId() == restartRebalanceCacheId)
                    return true;
            }
            return false;
        });

        IgniteEx node2 = startGrid(cfg2);
        spi2.waitForBlocked();

        IgniteEx filteredNode = startGrid(getTestIgniteInstanceName(nodeCnt) + filteredNodePostfix);

        for (CacheGroupContext grpCtx : filteredNode.context().cache().cacheGroups())
            grpCtx.preloader().rebalanceFuture().get(10_000);

        spi2.stopBlock();
        awaitPartitionMapExchange();
    }

    /**
     * Create and populate cluster.
     *
     * @param nodeCnt Node count.
     * @return Coordinator.
     * @throws Exception if any error occurs.
     */
    private IgniteEx createCluster(int nodeCnt) throws Exception {
        String grpName0 = "grp0";
        String grpName1 = "grp1";

        cacheCfgs = new CacheConfiguration[] {
            cacheConfiguration("ch_0_0", grpName0, 10, 2),
            cacheConfiguration("ch_0_1", grpName0, 10, 2),
            cacheConfiguration("ch_0_2", grpName0, 10, 2),
            cacheConfiguration("ch_1_0", grpName1, 10, 2),
            cacheConfiguration("ch_1_1", grpName1, 10, 2),
        };

        IgniteEx crd = startGrids(nodeCnt);
        crd.cluster().active(true);

        populateCluster(crd, 10, "");
        return crd;
    }

    /**
     * Сontent of node data on all partitions for all caches.
     *
     * @param node Node.
     * @param cnt  Count values.
     * @param add  Additional value postfix.
     */
    private void populateCluster(IgniteEx node, int cnt, String add) {
        requireNonNull(node);
        requireNonNull(add);

        for (CacheConfiguration cacheCfg : cacheCfgs) {
            String cacheName = cacheCfg.getName();
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < cacheCfg.getAffinity().partitions(); i++)
                partitionKeys(cache, i, cnt, i * cnt).forEach(k -> cache.put(k, cacheName + "_val_" + k + add));
        }
    }

    /**
     * Create cache configuration.
     *
     * @param cacheName Cache name.
     * @param grpName Cache group name.
     * @param parts Count of partitions.
     * @param backups Count backup.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName, @Nullable String grpName, int parts, int backups) {
        requireNonNull(cacheName);

        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(backups)
            .setGroupName(grpName);
    }

    /**
     * Restarting a node with log listeners.
     *
     * @param nodeId        Node id.
     * @param afterStop Function after stop node.
     * @param afterStart Function after start node.
     * @param checkConsumer Checking listeners.
     * @param logListeners  Log listeners.
     * @throws Exception if any error occurs.
     */
    private void restartNode(
        int nodeId,
        @Nullable Runnable afterStop,
        @Nullable Consumer<IgniteEx> afterStart,
        Consumer<LogListener> checkConsumer,
        LogListener... logListeners
    ) throws Exception {
        requireNonNull(checkConsumer);
        requireNonNull(logListeners);

        A.ensure(logListeners.length > 0, "Empty logListeners");

        for (LogListener rebLogListener : logListeners)
            rebLogListener.reset();

        stopGrid(nodeId);
        awaitPartitionMapExchange();

        if (nonNull(afterStop))
            afterStop.run();

        IgniteEx node = startGrid(nodeId);
        if(nonNull(afterStart))
            afterStart.accept(node);

        awaitPartitionMapExchange();

        for (LogListener rebLogListener : logListeners)
            checkConsumer.accept(rebLogListener);
    }
}
