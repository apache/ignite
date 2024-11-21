package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Stream.of;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;

/** Class for testing eviction results logging. */
public class LogEvictionResultsTest extends GridCommonAbstractTest {
    /** Listening logger. */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** Pattern for extracting number of partitions #1. */
    private final Pattern partsCntPattern = Pattern.compile("partitionsCount=(?<count>\\d+)");

    /** Pattern for extracting number of partitions #2. */
    private final Pattern partsPattern = Pattern.compile("partitions=(?<count>\\d+)");

    /** Pattern for counting partitions in log messages. */
    private final Pattern extractPartsPattern = Pattern.compile("partitions=\\[([^\\]]+)\\]");

    /** Persistence flag. */
    private boolean isPersistentCluster;

    /** Rebalance disabled flag. */
    private boolean isRebalanceDisabled;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setGroupName(DEFAULT_CACHE_NAME)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 64));

        if (isRebalanceDisabled)
            cacheCfg.setRebalanceMode(NONE);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setRebalanceThreadPoolSize(4)
            .setGridLogger(testLog)
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(cacheCfg);

        if (isPersistentCluster) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(1024 * 1024);
            dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(50 * 1024 * 1024);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /**
     * Test log messages for completion of clearing partitions during rebalance.
     */
    @Test
    public void testClearingDuringRebalance() throws Exception {
        List<String> rebalPrepMsgs = new ArrayList<>();
        List<String> rebalEvictMsgs = new ArrayList<>();
        List<String> chainComplMsgs = new ArrayList<>();
        List<String> chainEvictMsgs = new ArrayList<>();

        CallbackExecutorLogListener rebalPrepLsnr = new CallbackExecutorLogListener(
            "Prepared rebalancing.*", rebalPrepMsgs::add);

        CallbackExecutorLogListener rebalPrepEvictLsnr = new CallbackExecutorLogListener(
            "Following partitions have been successfully evicted in preparation for rebalancing.*",
            rebalEvictMsgs::add);

        CallbackExecutorLogListener chainComplLsnr = new CallbackExecutorLogListener(
            "Completed rebalance chain.*", chainComplMsgs::add);

        CallbackExecutorLogListener chainEvictLsnr = new CallbackExecutorLogListener(
            "Following partitions have been successfully evicted as part of rebalance chain.*",
            chainEvictMsgs::add);

        testLog.registerAllListeners(of(rebalPrepLsnr, chainComplLsnr, rebalPrepEvictLsnr, chainEvictLsnr)
            .toArray(LogListener[]::new));

        startTestGrids();

        List<Integer> rebalPrep = partsCount(rebalPrepMsgs, partsCntPattern);
        List<Integer> rebalEvict = partsCount(rebalEvictMsgs);

        assertTrue(F.containsAll(rebalPrep, rebalEvict) && F.containsAll(rebalEvict, rebalPrep));

        List<Integer> chainCompl = partsCount(chainComplMsgs, partsPattern);
        List<Integer> chainEvict = partsCount(chainEvictMsgs);

        assertTrue(F.containsAll(chainCompl, chainEvict) && F.containsAll(chainEvict, chainCompl));
    }

    /**
     * Test log messages for completion of eviction that may be required after rebalance.
     */
    @Test
    public void testCheckEviction() {
        String prepStr = "Partition has been scheduled for eviction \\((all affinity nodes are owners|this node " +
            "is oldest non-affinity node)\\).*";

        String evictStr = "Partitions have been successfullly evicted \\(reason for eviction: partitions did " +
            "not belong to affinity\\).*";

        checkLogMessages(prepStr, evictStr, () -> {
            try {
                startTestGrids();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Test log messages for completion of eviction with disabled rebalancing.
     */
    @Test
    public void testRebalanceDisabled() {
        String prepStr = "Evicting partition with rebalancing disabled \\(it does not belong to affinity\\).*";

        String evictStr = "Partitions have been successfullly evicted \\(reason for eviction: rebalancing " +
            "disabled, partitions did not belong to affinity\\).*";

        checkLogMessages(prepStr, evictStr, () -> {
            isRebalanceDisabled = true;

            try {
                startTestGrids();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Test log messages for completion of evicting partitions in the MOVING state.
     */
    @Test
    public void testEvictMovingPartitions() {
        String prepStr = "Evicting MOVING partition \\(it does not belong to affinity\\).*";

        String evictStr = "Partitions have been successfullly evicted \\(reason for eviction: moving partitions " +
            "not belonging to affinity\\).*";

        checkLogMessages(prepStr, evictStr, () -> {
            try {
                isPersistentCluster = true;

                startGrids(3);

                grid(0).cluster().state(ClusterState.ACTIVE);

                final List<Integer> evictedParts = evictingPartitionsAfterJoin(grid(2),
                    grid(2).cache(DEFAULT_CACHE_NAME), 3);

                final int keyCnt = 10;

                evictedParts.forEach(p ->
                    loadDataToPartition(p, getTestIgniteInstanceName(0), DEFAULT_CACHE_NAME, keyCnt, 0));

                forceCheckpoint();

                stopGrid(2);

                evictedParts.forEach(p ->
                    partitionKeys(grid(0).cache(DEFAULT_CACHE_NAME), p, keyCnt, 0)
                        .forEach(k -> grid(0).cache(DEFAULT_CACHE_NAME).remove(k)));

                CountDownLatch lock = new CountDownLatch(1);
                CountDownLatch unlock = new CountDownLatch(1);

                startGrid(2, new DependencyResolver() {
                    @Override public <T> T resolve(T instance) {
                        if (instance instanceof GridDhtPartitionTopologyImpl) {
                            GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)instance;

                            top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                                @Override public GridDhtLocalPartition create(
                                    GridCacheSharedContext ctx,
                                    CacheGroupContext grp,
                                    int id,
                                    boolean recovery
                                ) {
                                    return evictedParts.contains(id) ?
                                        new GridDhtLocalPartitionSyncEviction(ctx, grp, id, recovery, 2, lock, unlock) :
                                        new GridDhtLocalPartition(ctx, grp, id, recovery);
                                }
                            });
                        }

                        return instance;
                    }
                });

                assertTrue(U.await(lock, GridDhtLocalPartitionSyncEviction.TIMEOUT, TimeUnit.MILLISECONDS));

                startGrid(3);

                resetBaselineTopology();

                awaitPartitionMapExchange();

                unlock.countDown();

                awaitPartitionMapExchange();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** */
    public void startTestGrids() throws Exception {
        startGrids(2);
        awaitPartitionMapExchange();

        startGrid(2);
        awaitPartitionMapExchange();

        startGrid(3);
        awaitPartitionMapExchange();
    }

    /**
     * @param prepStr Log message string for preparing partitions for eviction.
     * @param evictStr Log message string for completion of eviction.
     * @param testTask Test task.
     */
    public void checkLogMessages(String prepStr, String evictStr, Runnable testTask) {
        setLoggerDebugLevel();

        List<String> partPreparedMsgs = new ArrayList<>();
        List<String> partsEvictedMsgs = new ArrayList<>();

        CallbackExecutorLogListener prepLsnr = new CallbackExecutorLogListener(prepStr, partPreparedMsgs::add);
        CallbackExecutorLogListener evictLsnr = new CallbackExecutorLogListener(evictStr, partsEvictedMsgs::add);

        testLog.registerAllListeners(of(prepLsnr, evictLsnr).toArray(LogListener[]::new));

        testTask.run();

        int allParts = partsCount(partsEvictedMsgs).stream().mapToInt(Integer::intValue).sum();

        assertEquals(allParts, partPreparedMsgs.size());
    }

    /**
     * @param list List of log messages.
     * @param pattern Pattern for extracting partitions count from log messages.
     */
    private List<Integer> partsCount(List<String> list, Pattern pattern) {
        List<Integer> res = new ArrayList<>();

        for (String msg : list) {
            Matcher matcher = pattern.matcher(msg);

            while (matcher.find())
                res.add(Integer.parseInt(matcher.group("count")));
        }

        return res;
    }

    /**
     * @param list List of log messages.
     */
    public List<Integer> partsCount(List<String> list) {
        List<Integer> res = new ArrayList<>();

        for (String msg : list) {
            Matcher matcher = extractPartsPattern.matcher(msg);

            int partsTotal = 0;

            while (matcher.find())
                partsTotal += matcher.group(1).split(",").length;

            res.add(partsTotal);
        }

        return res;
    }
}
