package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

/** Class for testing the logging of eviction results. */
public class LogEvictionResultsTest extends GridCommonAbstractTest {
    /** Number of keys to load into a partition. */
    private static final int KEY_CNT = 10;

    /** Number of partitions to move to the MOVING state. */
    private static final int PART_CNT = 3;

    /** Default pattern for extracting partitions count from log messages */
    private static final Pattern DEF_PARTS_CNT_PATTERN = Pattern.compile("partitionsCount=(?<count>\\d+)");

    /** Additional pattern for extracting partitions count from log messages */
    private static final Pattern ADD_PARTS_CNT_PATTERN = Pattern.compile("partitions=(?<count>\\d+)");

    /** Pattern for extracting partitions ids from log messages #1. */
    private static final Pattern P_PATTERN = Pattern.compile("p=(?<count>\\d+)");

    /** Pattern for extracting partitions ids from log messages #2. */
    private static final Pattern ID_PATTERN = Pattern.compile("id=(?<count>\\d+)");

    /** Pattern for extracting multiple partitions ids from log messages. */
    private static final Pattern EXTR_PARTS_PATTERN = Pattern.compile("partitions=\\[([^\\]]+)\\]");

    /** Listening test logger. */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** Latch for locking partition clearing. */
    private final CountDownLatch lock = new CountDownLatch(1);

    /** Latch for unlocking partition clearing. */
    private final CountDownLatch unlock = new CountDownLatch(1);

    /** Function to obtain a dependency resolver for launching a grid, allowing delayed partition clearing. */
    private final Function<List<Integer>, DependencyResolver> depResolverFunc = (evictedParts) ->
        new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)instance;

                    top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                        @Override public GridDhtLocalPartition create(GridCacheSharedContext ctx,
                            CacheGroupContext grp, int id, boolean recovery) {
                            return evictedParts.contains(id) ?
                                new GridDhtLocalPartitionSyncEviction(ctx, grp, id, recovery, 2, lock, unlock) :
                                new GridDhtLocalPartition(ctx, grp, id, recovery);
                        }
                    });
                }

                return instance;
            }
        };

    /** Flag indicating whether the cluster is configured as persistent. */
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
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        if (isRebalanceDisabled)
            cacheCfg.setRebalanceMode(NONE);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setRebalanceThreadPoolSize(PART_CNT)
            .setGridLogger(testLog)
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(cacheCfg);

        if (isPersistentCluster) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setWalSegmentSize(1024 * 1024);

            dsCfg.getDefaultDataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(50 * 1024 * 1024);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /**
     * Tests accuracy of log messages indicating completion of partition clearing during rebalancing.
     */
    @Test
    public void testClearingDuringRebalance() throws Exception {
        List<String> rebalPrepMsgs = new ArrayList<>();
        List<String> rebalEvictMsgs = new ArrayList<>();
        List<String> chainComplMsgs = new ArrayList<>();
        List<String> chainEvictMsgs = new ArrayList<>();

        CallbackExecutorLogListener rebalPrepLsnr = new CallbackExecutorLogListener(
            "Prepared rebalancing.*", rebalPrepMsgs::add);

        CallbackExecutorLogListener rebalEvictLsnr = new CallbackExecutorLogListener(
            "Following partitions have been successfully evicted in preparation for rebalancing.*",
            rebalEvictMsgs::add);

        CallbackExecutorLogListener chainComplLsnr = new CallbackExecutorLogListener(
            "Completed rebalance chain.*", chainComplMsgs::add);

        CallbackExecutorLogListener chainEvictLsnr = new CallbackExecutorLogListener(
            "Following partitions have been successfully evicted as part of rebalance chain.*",
            chainEvictMsgs::add);

        testLog.registerAllListeners(of(rebalPrepLsnr, rebalEvictLsnr, chainComplLsnr, chainEvictLsnr)
            .toArray(LogListener[]::new));

        startTestGrids();

        compareListsDisregardOrder(
            extractPartsCnt(rebalPrepMsgs),
            extractPartsCnt(rebalEvictMsgs));

        compareListsDisregardOrder(
            extractPartsCnt(chainComplMsgs, ADD_PARTS_CNT_PATTERN),
            extractPartsCnt(chainEvictMsgs));
    }

    /**
     * Tests accuracy of log messages indicating completion of eviction, which may be required after changes in topology.
     */
    @Test
    public void testCheckEviction() {
        String prepStr = "Partition has been scheduled for eviction \\((all affinity nodes are owners|this node " +
            "is oldest non-affinity node)\\).*";

        String evictStr = "Partitions have been successfullly evicted \\(eviction reason: partitions do " +
            "not belong to affinity\\).*";

        checkLogMessages(prepStr, evictStr, P_PATTERN, () -> {
            try {
                startTestGrids();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Tests accuracy of log messages indicating completion of eviction when rebalancing is disabled.
     */
    @Test
    public void testRebalanceDisabled() {
        String prepStr = "Evicting partition with rebalancing disabled \\(it does not belong to affinity\\).*";

        String evictStr = "Partitions have been successfullly evicted \\(eviction reason: rebalancing " +
            "disabled, partitions do not belong to affinity\\).*";

        checkLogMessages(prepStr, evictStr, ID_PATTERN, () -> {
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
     * Tests accuracy of log messages indicating completion of evicting partitions that are in the MOVING state.
     */
    @Test
    public void testEvictMovingPartitions() {
        String prepStr = "Evicting MOVING partition \\(it does not belong to affinity\\).*";

        String evictStr = "Partitions have been successfullly evicted \\(eviction reason: moving partitions, which " +
            "do not belong to affinity\\).*";

        checkLogMessages(prepStr, evictStr, P_PATTERN, () -> {
            try {
                isPersistentCluster = true;

                startGrids(3);

                grid(0).cluster().state(ClusterState.ACTIVE);

                final List<Integer> evictedParts = evictingPartitionsAfterJoin(grid(2),
                    grid(2).cache(DEFAULT_CACHE_NAME), PART_CNT);

                evictedParts.forEach(p ->
                    loadDataToPartition(p, getTestIgniteInstanceName(0), DEFAULT_CACHE_NAME, KEY_CNT, 0));

                forceCheckpoint();

                stopGrid(2);

                evictedParts.forEach(p ->
                    partitionKeys(grid(0).cache(DEFAULT_CACHE_NAME), p, KEY_CNT, 0)
                        .forEach(k -> grid(0).cache(DEFAULT_CACHE_NAME).remove(k)));

                startGrid(2, depResolverFunc.apply(evictedParts));

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
    private void startTestGrids() throws Exception {
        startGrids(2);
        awaitPartitionMapExchange();

        startGrid(2);
        awaitPartitionMapExchange();

        startGrid(3);
        awaitPartitionMapExchange();
    }

    /**
     * Checks log messages related to preparing and evicting partitions during a test task.
     *
     * @param prepStr Log message string indicating the preparation of partitions for eviction.
     * @param evictStr Log message string indicating the completion of partition eviction.
     * @param pattern Pattern used to extract partition information from debug log messages.
     * @param testTask Test task to be executed during the logging process.
     */
    private void checkLogMessages(String prepStr, String evictStr, Pattern pattern, Runnable testTask) {
        setLoggerDebugLevel();

        List<String> partsPreparedMsgs = new ArrayList<>();
        List<String> partsEvictedMsgs = new ArrayList<>();

        CallbackExecutorLogListener prepLsnr = new CallbackExecutorLogListener(prepStr, partsPreparedMsgs::add);
        CallbackExecutorLogListener evictLsnr = new CallbackExecutorLogListener(evictStr, partsEvictedMsgs::add);

        testLog.registerAllListeners(of(prepLsnr, evictLsnr).toArray(LogListener[]::new));

        testTask.run();

        compareListsDisregardOrder(
            extractParts(partsPreparedMsgs, pattern),
            extractParts(partsEvictedMsgs, EXTR_PARTS_PATTERN));
    }

    /**
     * @param logMsgs List of log messages.
     */
    private List<Integer> extractPartsCnt(List<String> logMsgs) {
        return extractPartsCnt(logMsgs, DEF_PARTS_CNT_PATTERN);
    }

    /**
     * @param logMsgs List of log messages.
     * @param pattern Pattern used to extract partitions count from log messages.
     */
    private List<Integer> extractPartsCnt(List<String> logMsgs, Pattern pattern) {
        List<Integer> res = new ArrayList<>();

        for (String msg : logMsgs) {
            Matcher matcher = pattern.matcher(msg);

            int sumForCurMsg = 0;

            while (matcher.find())
                sumForCurMsg += Integer.parseInt(matcher.group("count"));

            res.add(sumForCurMsg);
        }

        return res;
    }

    /**
     * @param logMsgs List of log messages.
     * @param pattern Pattern used to extract partitions from debug log messages.
     */
    private List<Integer> extractParts(List<String> logMsgs, Pattern pattern) {
        List<Integer> res = new ArrayList<>();

        for (String msg : logMsgs) {
            Matcher matcher = pattern.matcher(msg);

            if (matcher.find()) {
                String[] parts = matcher.group(1).split(",");

                for (String part : parts)
                    res.add(Integer.parseInt(part.trim()));
            }
        }

        return res;
    }

    /**
     * Asserts that two lists contain the same integer elements, regardless of order.
     *
     * @param list1 List one.
     * @param list2 List two.
     */
    private void compareListsDisregardOrder(List<Integer> list1, List<Integer> list2) {
        assertTrue( F.containsAll(list1, list2) && F.containsAll(list2, list1));
    }
}
