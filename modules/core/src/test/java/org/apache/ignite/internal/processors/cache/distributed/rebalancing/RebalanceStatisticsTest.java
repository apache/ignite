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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplier;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceStatistics;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.SupplierRebalanceStatistics;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridTuple4;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T4;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.lang.Boolean.FALSE;
import static java.lang.System.setProperty;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceStatisticsUtils.availablePrintRebalanceStatistics;
import static org.apache.ignite.internal.util.IgniteUtils.currentTimeMillis;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 * For testing of rebalance statistics.
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
     * Test checks that rebalance statistics are output into log only if
     * {@link IgniteSystemProperties#IGNITE_QUIET} == {@code false}.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    @WithSystemProperty(key = IGNITE_QUIET, value = "true")
    public void testPrintIntoLogRebStatDependSysProps() throws Exception {
        LogListener[] logListeners = {
            matches(new GrpStatPred()).build(),
            matches(new TotalStatPred()).build(),
            matches(compile("Rebalance duration was greater than.*partitions distribution.*")).build()
        };

        listenLog.registerAllListeners(logListeners);

        int nodeId = 0;
        startGrid(nodeId++);

        assertFalse(availablePrintRebalanceStatistics());
        restartNode(nodeId, null, null, l -> assertFalse(l.check()), logListeners);

        setProperty(IGNITE_QUIET, FALSE.toString());
        assertTrue(availablePrintRebalanceStatistics());
        restartNode(nodeId, null, null, l -> assertEquals(l != logListeners[2], l.check()), logListeners);
    }

    /**
     * Test statistics of a full rebalance.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    public void testFullRebalanceStatistics() throws Exception {
        createCluster(3);

        int restartNodeId = 2;
        Map<String, RebalanceStatistics> expGrpStats = calcGrpStat(restartNodeId);

        Map<String, RebalanceStatistics> expTotalStats = new HashMap<>();
        updateTotalStat(expTotalStats, expGrpStats);

        GrpStatPred grpStatPred = new GrpStatPred();
        TotalStatPred totalStatPred = new TotalStatPred();

        LogListener[] logListeners = {
            matches(grpStatPred).build(),
            matches(totalStatPred).build()
        };

        long beforeRestartNode = currentTimeMillis();

        listenLog.registerAllListeners(logListeners);
        restartNode(restartNodeId, null, null, l -> assertTrue(l.check()), logListeners);

        long afterRestartNode = currentTimeMillis();

        checkStat(
            restartNodeId,
            expGrpStats,
            expTotalStats,
            grpStatPred,
            totalStatPred,
            null,
            beforeRestartNode,
            afterRestartNode,
            false
        );
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

        GrpStatPred grpStatPred = new GrpStatPred();
        TotalStatPred totalStatPred = new TotalStatPred();

        LogListener[] logListeners = {
            matches(grpStatPred).build(),
            matches(totalStatPred).build()
        };

        listenLog.registerAllListeners(logListeners);

        int restartNodeId = 2;
        AtomicReference<Map<String, RebalanceStatistics>> calcGrpStatRef = new AtomicReference<>();

        long beforeRestartNode = currentTimeMillis();

        restartNode(
            restartNodeId,
            () -> populateCluster(crd, 10, "_"),
            node -> {
                AffinityTopologyVersion waitTopVer = crd.context().discovery().topologyVersionEx();
                node.context().cache().context().exchange().affinityReadyFuture(waitTopVer).listen(f -> {
                    try {
                        calcGrpStatRef.set(calcGrpStat(restartNodeId));
                    }
                    catch (Exception e) {
                        throw new IgniteException(e);
                    }
                });
            },
            l -> assertTrue(l.check()),
            logListeners
        );

        long afterRestartNode = currentTimeMillis();

        Map<String, RebalanceStatistics> expGrpStats = calcGrpStatRef.get();
        assertNotNull(expGrpStats);

        Map<String, RebalanceStatistics> expTotalStats = new HashMap<>();
        updateTotalStat(expTotalStats, expGrpStats);

        expGrpStats.remove(UTILITY_CACHE_NAME);
        expTotalStats.remove(UTILITY_CACHE_NAME);

        checkStat(
            restartNodeId,
            expGrpStats,
            expTotalStats,
            grpStatPred,
            totalStatPred,
            grpCtx -> !UTILITY_CACHE_NAME.equals(grpCtx.cacheOrGroupName()),
            beforeRestartNode,
            afterRestartNode,
            grpStatPred.values.stream().map(GridTuple4::get2).noneMatch(CacheGroupContext::mvccEnabled)
        );
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

        GrpStatPred grpStatPred = new GrpStatPred();
        TotalStatPred totalStatPred = new TotalStatPred();

        LogListener[] logListeners = {
            matches(grpStatPred).build(),
            matches(totalStatPred).build(),
            matches(Exception.class.getSimpleName()).build()
        };

        listenLog.registerAllListeners(logListeners);

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

        assertTrue(logListeners[0].check());
        assertTrue(logListeners[1].check());
        assertFalse(logListeners[2].check());

        assertEquals(2, totalStatPred.values.size());

        Map<IgniteEx, List<GrpStat>> statByNodes = grpStatPred.values.stream()
            .collect(groupingBy(GridTuple4::get1, LinkedHashMap::new, toList()));

        assertEquals(2, statByNodes.size());
        assertTrue(statByNodes.containsKey(filteredNode));
        assertTrue(statByNodes.containsKey(node2));

        for (GrpStat grpStat : statByNodes.get(filteredNode)) {
            assertTrue(grpStat.get3().get());
            assertEquals(1, grpStat.get4().attempt());
        }

        List<GrpStat> restartedRebalanceCacheStats = new ArrayList<>();

        for (GrpStat grpStat : statByNodes.get(node2)) {
            int cacheId = cacheId(grpStat.get2().cacheOrGroupName());

            if (restartRebalanceCacheId == cacheId)
                restartedRebalanceCacheStats.add(grpStat);
            else {
                assertTrue(grpStat.get3().get());
                assertEquals(cacheId(UTILITY_CACHE_NAME) == cacheId ? 1 : 2, grpStat.get4().attempt());
            }
        }

        assertEquals(2, restartedRebalanceCacheStats.size());
    }

    /**
     * Checking that duration is not negative.
     *
     * @param stat Cache group rebaslance statistics.
     */
    private void checkDuration(RebalanceStatistics stat) {
        requireNonNull(stat);

        assertTrue((stat.end() - stat.start()) >= 0);

        stat.supplierStatistics().values().forEach(supStat -> assertTrue((supStat.end() - supStat.start()) >= 0));
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

    /**
     * Сalculation of expected statistics of rebalance for cache groups.
     *
     * @param nodeId Node id.
     * @return Rebalance statistics for cache groups.
     * @throws Exception if any error occurs.
     */
    private Map<String, RebalanceStatistics> calcGrpStat(int nodeId) throws Exception {
        Map<String, RebalanceStatistics> grpStats = new HashMap<>();

        for (CacheGroupContext grpCtx : grid(nodeId).context().cache().cacheGroups()) {
            RebalanceStatistics grpStat = new RebalanceStatistics();
            grpStats.put(grpCtx.cacheOrGroupName(), grpStat);

            Map<GridDhtLocalPartition, GridDhtPartitionState> locPartState = new HashMap<>();

            for (GridDhtLocalPartition locPart : grpCtx.topology().localPartitions()) {
                locPartState.put(locPart, locPart.state());
                locPart.setState(GridDhtPartitionState.MOVING);
            }

            GridDhtPartitionsExchangeFuture exchFut = grpCtx.shared().exchange().lastTopologyFuture();
            GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

            if (exchFut.rebalanced())
                U.findField(exchFut.getClass(), "rebalanced").set(exchFut, false);

            GridDhtPreloaderAssignments assigns = grpCtx.preloader().generateAssignments(exchId, exchFut);
            for (Entry<ClusterNode, GridDhtPartitionDemandMessage> assignEntry : assigns.entrySet()) {
                IgniteEx supplierNode = (IgniteEx)grid(assignEntry.getKey());
                CacheGroupContext supGrpCtx = supplierNode.context().cache().cacheGroup(grpCtx.groupId());
                GridDhtPartitionSupplier supplier = ((GridDhtPreloader)supGrpCtx.preloader()).supplier();

                Method extractEntryInfo = U.findNonPublicMethod(
                    supplier.getClass(),
                    "extractEntryInfo",
                    CacheDataRow.class
                );

                GridDhtPartitionDemandMessage demandMsg = assignEntry.getValue();

                Set<Integer> remainingParts = new HashSet<>(demandMsg.partitions().fullSet());
                remainingParts.addAll(demandMsg.partitions().historicalSet());

                IgniteRebalanceIterator rebIter = supGrpCtx.offheap().rebalanceIterator(
                    demandMsg.partitions(),
                    demandMsg.topologyVersion()
                );

                while (rebIter.hasNext()) {
                    CacheDataRow row = rebIter.next();

                    int partId = row.partition();

                    GridCacheEntryInfo cacheEntryInfo = (GridCacheEntryInfo)extractEntryInfo.invoke(supplier, row);
                    int bytes = cacheEntryInfo.marshalledSize(supGrpCtx.cacheObjectContext());

                    grpStat.update(supplierNode.localNode(), partId, rebIter.historical(partId), 1, bytes);
                    remainingParts.remove(partId);
                }

                for (Integer remPartId : remainingParts)
                    grpStat.update(supplierNode.localNode(), remPartId, rebIter.historical(remPartId), 0, 0);
            }

            locPartState.forEach(GridDhtLocalPartition::setState);
        }
        return grpStats;
    }

    /**
     * Update total rebalance statistics.
     *
     * @param totalStats Total rebalance statistics.
     * @param grpStats   Cache group rebalance statistics.
     */
    private void updateTotalStat(
        Map<String, RebalanceStatistics> totalStats,
        Map<String, RebalanceStatistics> grpStats
    ) {
        requireNonNull(totalStats);
        requireNonNull(grpStats);

        for (Entry<String, RebalanceStatistics> e : grpStats.entrySet())
            totalStats.computeIfAbsent(e.getKey(), s -> new RebalanceStatistics()).merge(e.getValue());
    }

    /**
     * Checking statistic rebalance.
     *
     * @param nodeId Node id.
     * @param expGrpStats Expected group statistics.
     * @param expTotalStats Expected total statistics.
     * @param grpStatPred Actual group statistics.
     * @param totalStatPred Actual total statistics.
     * @param grpFilter Cache group filter.
     * @param expStart Expected start time.
     * @param expEnd Expected end time.
     * @param hist Historical rebalance.
     */
    private void checkStat(
        int nodeId,
        Map<String, RebalanceStatistics> expGrpStats,
        Map<String, RebalanceStatistics> expTotalStats,
        GrpStatPred grpStatPred,
        TotalStatPred totalStatPred,
        @Nullable Predicate<CacheGroupContext> grpFilter,
        long expStart,
        long expEnd,
        boolean hist
    ) throws IgniteCheckedException {
        requireNonNull(expGrpStats);
        requireNonNull(expTotalStats);
        requireNonNull(grpStatPred);
        requireNonNull(totalStatPred);

        //checking that only for nodeId=2 had statistics into log
        Set<String> nodes = grpStatPred.values.stream().map(GridTuple4::get1).map(Ignite::name).collect(toSet());
        totalStatPred.values.stream().map(IgniteBiTuple::get1).map(Ignite::name).forEach(nodes::add);

        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(grid(nodeId).name()));

        int actGrpStatSize = nonNull(grpFilter) ?
            (int)grpStatPred.values.stream().map(GridTuple4::get2).filter(grpFilter).count() :
            grpStatPred.values.size();

        assertEquals(expGrpStats.size(), actGrpStatSize);
        assertTrue(actGrpStatSize > 0);

        for (T4<IgniteEx, CacheGroupContext, RebalanceFuture, RebalanceStatistics> t4 : grpStatPred.values) {
            if (nonNull(grpFilter) && !grpFilter.test(t4.get2()))
                continue;

            //check that result was successful
            assertTrue(t4.get3().get());

            RebalanceStatistics actGrpStat = t4.get4();
            assertEquals(1, actGrpStat.attempt());

            RebalanceStatistics expGrpStat = expGrpStats.get(t4.get2().cacheOrGroupName());
            checkGrpStat(expGrpStat, actGrpStat, expStart, expEnd, hist);
        }

        for (T2<IgniteEx, Map<CacheGroupContext, RebalanceStatistics>> t2 : totalStatPred.values) {
            Map<CacheGroupContext, RebalanceStatistics> actTotalStats = t2.get2();

            int actTotalStatSize = nonNull(grpFilter) ?
                (int)actTotalStats.keySet().stream().filter(grpFilter).count() : actTotalStats.size();

            assertEquals(expTotalStats.size(), actTotalStatSize);
            assertTrue(actTotalStatSize > 0);

            for (Entry<CacheGroupContext, RebalanceStatistics> actTotalStatE : actTotalStats.entrySet()) {
                if (nonNull(grpFilter) && !grpFilter.test(actTotalStatE.getKey()))
                    continue;

                checkTotalStat(
                    expTotalStats.get(actTotalStatE.getKey().cacheOrGroupName()),
                    actTotalStatE.getValue(),
                    expStart,
                    expEnd
                );
            }
        }
    }

    /**
     * Checking equality of {@code exp} and {@code act}.
     *
     * @param exp Expected rebalance statistics.
     * @param act Actual rebalance statistics.
     * @param expStart Expected start time.
     * @param expEnd Expected end time.
     * @param hist Historical rebalance.
     */
    private void checkGrpStat(
        RebalanceStatistics exp,
        RebalanceStatistics act,
        long expStart,
        long expEnd,
        boolean hist
    ) {
        assertNotNull(exp);
        assertNotNull(act);

        checkTime(expStart, expEnd, act.start(), act.end());

        Map<ClusterNode, SupplierRebalanceStatistics> expSupStats = exp.supplierStatistics();
        Map<ClusterNode, SupplierRebalanceStatistics> actSupStats = act.supplierStatistics();

        assertEquals(expSupStats.size(), actSupStats.size());
        assertFalse(actSupStats.isEmpty());

        for (Entry<ClusterNode, SupplierRebalanceStatistics> expSupStatE : expSupStats.entrySet()) {
            SupplierRebalanceStatistics expSupStat = expSupStatE.getValue();
            SupplierRebalanceStatistics actSupStat = actSupStats.get(expSupStatE.getKey());

            assertNotNull(actSupStat);

            checkTime(expStart, expEnd, actSupStat.start(), actSupStat.end());

            assertEquals(expSupStat.partitions(), actSupStat.partitions());
            actSupStat.partitions().values().forEach(p -> assertEquals(!hist, (boolean) p));

            assertEquals(expSupStat.fullEntries(), actSupStat.fullEntries());
            assertEquals(expSupStat.histEntries(), actSupStat.histEntries());

            assertEquals(expSupStat.fullBytes(), actSupStat.fullBytes());
            assertEquals(expSupStat.histBytes(), actSupStat.histBytes());
        }
    }

    /**
     * Checking equality of {@code exp} and {@code act}.
     *
     * @param exp Expected total rebalance statistics.
     * @param act Actual total rebalance statistics.
     * @param expStart Expected start time.
     * @param expEnd Expected end time.
     */
    private void checkTotalStat(
        RebalanceStatistics exp,
        RebalanceStatistics act,
        long expStart,
        long expEnd
    ) {
        assertNotNull(exp);
        assertNotNull(act);

        checkTime(expStart, expEnd, act.start(), act.end());

        Map<ClusterNode, SupplierRebalanceStatistics> expSupStats = exp.supplierStatistics();
        Map<ClusterNode, SupplierRebalanceStatistics> actSupStats = act.supplierStatistics();

        assertEquals(expSupStats.size(), actSupStats.size());
        assertFalse(actSupStats.isEmpty());

        for (Entry<ClusterNode, SupplierRebalanceStatistics> expSupStatE : expSupStats.entrySet()) {
            SupplierRebalanceStatistics expSupStat = expSupStatE.getValue();
            SupplierRebalanceStatistics actSupStat = actSupStats.get(expSupStatE.getKey());

            assertNotNull(actSupStat);

            checkTime(expStart, expEnd, actSupStat.start(), actSupStat.end());

            assertEquals(expSupStat.fullParts(), actSupStat.fullParts());
            assertEquals(expSupStat.histParts(), actSupStat.histParts());

            assertEquals(expSupStat.fullEntries(), actSupStat.fullEntries());
            assertEquals(expSupStat.histEntries(), actSupStat.histEntries());

            assertEquals(expSupStat.fullBytes(), actSupStat.fullBytes());
            assertEquals(expSupStat.histBytes(), actSupStat.histBytes());
        }
    }

    /**
     * Time check that {@code actStart} and {@code actEnd} are in between
     * {@code expStart} and {@code expEnd}, and the duration is positive.
     *
     * @param expStart Expected start time.
     * @param expEnd Expected end time.
     * @param actStart Actual start time.
     * @param actEnd Actual end time.
     */
    private void checkTime(long expStart, long expEnd, long actStart, long actEnd) {
        assertTrue(actStart >= expStart && actStart <= expEnd);
        assertTrue(actEnd >= expStart && actEnd <= expEnd);
        assertTrue((actEnd - actStart) >= 0);
    }

    /**
     * Class container for statistics of rebalance for cache group.
     */
    static class GrpStat extends T4<IgniteEx, CacheGroupContext, RebalanceFuture, RebalanceStatistics> {
        /**
         * Constructor.
         *
         * @param val1 Node.
         * @param val2 Cache group context.
         * @param val3 Rebalance future.
         * @param val4 Rebalance statistics.
         */
        public GrpStat(IgniteEx val1, CacheGroupContext val2, RebalanceFuture val3, RebalanceStatistics val4) {
            super(val1, val2, val3, val4);
        }
    }

    /**
     * Predicate for getting rebalance statistics for cache group when listening log.
     */
    class GrpStatPred extends StatPred<GrpStat> {
        /**
         * Default constructor.
         */
        public GrpStatPred() {
            super(
                compile("Rebalance information per cache group \\(.* rebalance\\): \\[id=.*, name=(.*?), startTime=.*")
            );
        }

        /** {@inheritDoc} */
        @Override public GrpStat value(
            Matcher m,
            IgniteThread t
        ) {
            IgniteEx node = grid(t.getIgniteInstanceName());
            CacheGroupContext grpCtx = node.context().cache().cacheGroup(cacheId(m.group(1)));
            RebalanceFuture rebFut = (RebalanceFuture)grpCtx.preloader().rebalanceFuture();

            return new GrpStat(node, grpCtx, rebFut, new RebalanceStatistics(rebFut.statistics()));
        }
    }

    /**
     * Predicate for getting total rebalance statistics for all cache group when listening log.
     */
    class TotalStatPred extends StatPred<T2<IgniteEx, Map<CacheGroupContext, RebalanceStatistics>>> {
        /**
         * Default constructor.
         */
        public TotalStatPred() {
            super(compile("Rebalance total information \\(including successful and not rebalances\\):.*"));
        }

        /** {@inheritDoc} */
        @Override public T2<IgniteEx, Map<CacheGroupContext, RebalanceStatistics>> value(
            Matcher m,
            IgniteThread t
        ) {
            IgniteEx node = grid(t.getIgniteInstanceName());

            Map<CacheGroupContext, RebalanceStatistics> stat = new HashMap<>();
            for (CacheGroupContext grpCtx : node.context().cache().cacheGroups()) {
                GridDhtPartitionDemander demander = ((GridDhtPreloader)grpCtx.preloader()).demander();
                stat.put(grpCtx, new RebalanceStatistics(demander.totalStatistics()));
            }
            return new T2<>(node, stat);
        }
    }

    /**
     * Base predicate for getting rebalance statistics when listening log.
     */
    private abstract class StatPred<T> implements Predicate<String> {
        /** Pattern for finding statistics of rebalance. */
        final Pattern ptrn;

        /** Obtained values. */
        final Collection<T> values = new ConcurrentLinkedQueue<>();

        /**
         * Constructor.
         *
         * @param ptrn Pattern for finding statistics of rebalance.
         */
        public StatPred(Pattern ptrn) {
            requireNonNull(ptrn);

            this.ptrn = ptrn;
        }

        /**
         * Creating a special value for found statistics.
         *
         * @param m Statistics matcher.
         * @param t Thread of found statistics.
         * @return Special value for found statistics.
         */
        public abstract T value(Matcher m, IgniteThread t);

        /** {@inheritDoc} */
        @Override public boolean test(String logStr) {
            Matcher matcher = ptrn.matcher(logStr);
            if (matcher.matches()) {
                values.add(value(matcher, (IgniteThread)Thread.currentThread()));

                return true;
            }
            return false;
        }
    }
}
