package org.gridgain.grid.kernal.processors.cache;

import com.google.common.collect.*;
import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.failover.*;
import org.apache.ignite.spi.failover.always.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests putAll() method along with failover and different configurations.
 */
public class GridCachePutAllFailoverSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Size of the test map. */
    private static final int TEST_MAP_SIZE = 100000;

    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Size of data chunk, sent to a remote node. */
    private static final int DATA_CHUNK_SIZE = 1000;

    /** Number of chunk on which to fail worker node. */
    public static final int FAIL_ON_CHUNK_NO = (TEST_MAP_SIZE / DATA_CHUNK_SIZE) / 3;

    /** Await timeout in seconds. */
    public static final int AWAIT_TIMEOUT_SEC = 65;

    /** */
    private static final int FAILOVER_PUSH_GAP = 30;

    /** Master node name. */
    private static final String MASTER = "master";

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** Backups count. */
    private int backups;

    /** Filter to include only worker nodes. */
    private static final IgnitePredicate<ClusterNode> workerNodesFilter = new PN() {
        @SuppressWarnings("unchecked")
        @Override public boolean apply(ClusterNode n) {
             return "worker".equals(n.attribute("segment"));
        }
    };

    /**
     * Result future queue (restrict the queue size
     * to 50 in order to prevent in-memory data grid from over loading).
     */
    private final BlockingQueue<ComputeTaskFuture<?>> resQueue = new LinkedBlockingQueue<>(50);

    /** Test failover SPI. */
    private MasterFailoverSpi failoverSpi = new MasterFailoverSpi((IgnitePredicate)workerNodesFilter);

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverColocatedNearEnabledThreeBackups() throws Exception {
        checkPutAllFailoverColocated(true, 7, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverColocatedNearDisabledThreeBackups() throws Exception {
        checkPutAllFailoverColocated(false, 7, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverNearEnabledOneBackup() throws Exception {
        checkPutAllFailover(true, 3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverNearDisabledOneBackup() throws Exception {
        checkPutAllFailover(false, 3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverNearEnabledTwoBackups() throws Exception {
        checkPutAllFailover(true, 5, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverNearDisabledTwoBackups() throws Exception {
        checkPutAllFailover(false, 5, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverNearEnabledThreeBackups() throws Exception {
        checkPutAllFailover(true, 7, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverNearDisabledThreeBackups() throws Exception {
        checkPutAllFailover(false, 7, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverColocatedNearEnabledOneBackup() throws Exception {
        checkPutAllFailoverColocated(true, 3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverColocatedNearDisabledOneBackup() throws Exception {
        checkPutAllFailoverColocated(false, 3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverColocatedNearEnabledTwoBackups() throws Exception {
        checkPutAllFailoverColocated(true, 5, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverColocatedNearDisabledTwoBackups() throws Exception {
        checkPutAllFailoverColocated(false, 5, 2);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 5;
    }

    /**
     * Tests putAll() method along with failover and cache backup.
     *
     * Checks that the resulting primary cache size is the same as
     * expected.
     *
     * @param near Near enabled.
     * @param workerCnt Worker count.
     * @param shutdownCnt Shutdown count.
     * @throws Exception If failed.
     */
    public void checkPutAllFailover(boolean near, int workerCnt, int shutdownCnt) throws Exception {
        nearEnabled = near;
        backups = shutdownCnt;

        Collection<Integer> testKeys = generateTestKeys();

        final Ignite master = startGrid(MASTER);

        List<Ignite> workers = new ArrayList<>(workerCnt);

        for (int i = 1; i <= workerCnt; i++)
            workers.add(startGrid("worker" + i));

        info("Master: " + master.cluster().localNode().id());

        List<Ignite> runningWorkers = new ArrayList<>(workerCnt);

        for (int i = 1; i <= workerCnt; i++) {
            UUID id = workers.get(i - 1).cluster().localNode().id();

            info(String.format("Worker%d - %s", i, id));

            runningWorkers.add(workers.get(i - 1));
        }

        try {
            // Dummy call to fetch affinity function from remote node
            master.cluster().mapKeyToNode(CACHE_NAME, "Dummy");

            Random rnd = new Random();

            Collection<Integer> dataChunk = new ArrayList<>(DATA_CHUNK_SIZE);
            int entryCntr = 0;
            int chunkCntr = 0;
            final AtomicBoolean jobFailed = new AtomicBoolean(false);

            int failoverPushGap = 0;

            final CountDownLatch emptyLatch = new CountDownLatch(1);

            final AtomicBoolean inputExhausted = new AtomicBoolean();

            IgniteCompute comp = compute(master.cluster().forPredicate(workerNodesFilter)).enableAsync();

            for (Integer key : testKeys) {
                dataChunk.add(key);
                entryCntr++;

                if (entryCntr == DATA_CHUNK_SIZE) { // time to send data
                    chunkCntr++;

                    assert dataChunk.size() == DATA_CHUNK_SIZE;

                    log.info("Pushing data chunk [chunkNo=" + chunkCntr + "]");

                    comp.execute(
                        new GridCachePutAllTask(
                            runningWorkers.get(rnd.nextInt(runningWorkers.size())).cluster().localNode().id(),
                            CACHE_NAME),
                            dataChunk);

                    ComputeTaskFuture<Void> fut = comp.future();

                    resQueue.put(fut); // Blocks if queue is full.

                    fut.listenAsync(new CI1<IgniteFuture<Void>>() {
                        @Override public void apply(IgniteFuture<Void> f) {
                            ComputeTaskFuture<?> taskFut = (ComputeTaskFuture<?>)f;

                            try {
                                taskFut.get(); //if something went wrong - we'll get exception here
                            }
                            catch (IgniteCheckedException e) {
                                log.error("Job failed", e);

                                jobFailed.set(true);
                            }

                            // Remove complete future from queue to allow other jobs to proceed.
                            resQueue.remove(taskFut);

                            if (inputExhausted.get() && resQueue.isEmpty())
                                emptyLatch.countDown();
                        }
                    });

                    entryCntr = 0;
                    dataChunk = new ArrayList<>(DATA_CHUNK_SIZE);

                    if (chunkCntr >= FAIL_ON_CHUNK_NO) {
                        if (workerCnt - runningWorkers.size() < shutdownCnt) {
                            if (failoverPushGap > 0)
                                failoverPushGap--;
                            else {
                                Ignite victim = runningWorkers.remove(0);

                                info("Shutting down node: " + victim.cluster().localNode().id());

                                stopGrid(victim.name());

                                // Fail next node after some jobs have been pushed.
                                failoverPushGap = FAILOVER_PUSH_GAP;
                            }
                        }
                    }
                }
            }

            inputExhausted.set(true);

            if (resQueue.isEmpty())
                emptyLatch.countDown();

            assert chunkCntr == TEST_MAP_SIZE / DATA_CHUNK_SIZE;

            // Wait for queue to empty.
            log.info("Waiting for empty queue...");

            boolean failedWait = false;

            if (!emptyLatch.await(AWAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                info(">>> Failed to wait for queue to empty.");

                failedWait = true;
            }

            if (!failedWait)
                assertFalse("One or more jobs have failed.", jobFailed.get());

            Collection<Integer> absentKeys = findAbsentKeys(runningWorkers.get(0), testKeys);

            if (!failedWait && !absentKeys.isEmpty()) {
                // Give some time to preloader.
                U.sleep(15000);

                absentKeys = findAbsentKeys(runningWorkers.get(0), testKeys);
            }

            info(">>> Absent keys: " + absentKeys);

            assertTrue(absentKeys.isEmpty());

            // Actual primary cache size.
            int primaryCacheSize = 0;

            for (Ignite g : runningWorkers) {
                info(">>>>> " + g.cache(CACHE_NAME).size());

                primaryCacheSize += g.cache(CACHE_NAME).primarySize();
            }

            assertEquals(TEST_MAP_SIZE, primaryCacheSize);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests putAll() method along with failover and cache backup.
     *
     * Checks that the resulting primary cache size is the same as
     * expected.
     *
     * @param near Near enabled.
     * @param workerCnt Worker count.
     * @param shutdownCnt Shutdown count.
     * @throws Exception If failed.
     */
    public void checkPutAllFailoverColocated(boolean near, int workerCnt, int shutdownCnt) throws Exception {
        nearEnabled = near;
        backups = shutdownCnt;

        Collection<Integer> testKeys = generateTestKeys();

        final Ignite master = startGrid(MASTER);

        List<Ignite> workers = new ArrayList<>(workerCnt);

        for (int i = 1; i <= workerCnt; i++)
            workers.add(startGrid("worker" + i));

        info("Master: " + master.cluster().localNode().id());

        List<Ignite> runningWorkers = new ArrayList<>(workerCnt);

        for (int i = 1; i <= workerCnt; i++) {
            UUID id = workers.get(i - 1).cluster().localNode().id();

            info(String.format("Worker%d: %s", i, id));

            runningWorkers.add(workers.get(i - 1));
        }

        try {
            // Dummy call to fetch affinity function from remote node
            master.cluster().mapKeyToNode(CACHE_NAME, "Dummy");

            Map<UUID, Collection<Integer>> dataChunks = new HashMap<>();

            int chunkCntr = 0;
            final AtomicBoolean jobFailed = new AtomicBoolean(false);

            int failoverPushGap = 0;

            final CountDownLatch emptyLatch = new CountDownLatch(1);

            final AtomicBoolean inputExhausted = new AtomicBoolean();

            IgniteCompute comp = compute(master.cluster().forPredicate(workerNodesFilter)).enableAsync();

            for (Integer key : testKeys) {
                ClusterNode mappedNode = master.cluster().mapKeyToNode(CACHE_NAME, key);

                UUID nodeId = mappedNode.id();

                Collection<Integer> data = dataChunks.get(nodeId);

                if (data == null) {
                    data = new ArrayList<>(DATA_CHUNK_SIZE);

                    dataChunks.put(nodeId, data);
                }

                data.add(key);

                if (data.size() == DATA_CHUNK_SIZE) { // time to send data
                    chunkCntr++;

                    log.info("Pushing data chunk [chunkNo=" + chunkCntr + "]");

                    comp.execute(new GridCachePutAllTask(nodeId, CACHE_NAME), data);

                    ComputeTaskFuture<Void> fut = comp.future();

                    resQueue.put(fut); // Blocks if queue is full.

                    fut.listenAsync(new CI1<IgniteFuture<Void>>() {
                        @Override public void apply(IgniteFuture<Void> f) {
                            ComputeTaskFuture<?> taskFut = (ComputeTaskFuture<?>)f;

                            try {
                                taskFut.get(); //if something went wrong - we'll get exception here
                            }
                            catch (IgniteCheckedException e) {
                                log.error("Job failed", e);

                                jobFailed.set(true);
                            }

                            // Remove complete future from queue to allow other jobs to proceed.
                            resQueue.remove(taskFut);

                            if (inputExhausted.get() && resQueue.isEmpty())
                                emptyLatch.countDown();
                        }
                    });

                    data = new ArrayList<>(DATA_CHUNK_SIZE);

                    dataChunks.put(nodeId, data);

                    if (chunkCntr >= FAIL_ON_CHUNK_NO) {
                        if (workerCnt - runningWorkers.size() < shutdownCnt) {
                            if (failoverPushGap > 0)
                                failoverPushGap--;
                            else {
                                Ignite victim = runningWorkers.remove(0);

                                info("Shutting down node: " + victim.cluster().localNode().id());

                                stopGrid(victim.name());

                                // Fail next node after some jobs have been pushed.
                                failoverPushGap = FAILOVER_PUSH_GAP;
                            }
                        }
                    }
                }
            }

            for (Map.Entry<UUID, Collection<Integer>> entry : dataChunks.entrySet()) {
                comp.execute(new GridCachePutAllTask(entry.getKey(), CACHE_NAME), entry.getValue());

                ComputeTaskFuture<Void> fut = comp.future();

                resQueue.put(fut); // Blocks if queue is full.

                fut.listenAsync(new CI1<IgniteFuture<Void>>() {
                    @Override public void apply(IgniteFuture<Void> f) {
                        ComputeTaskFuture<?> taskFut = (ComputeTaskFuture<?>)f;

                        try {
                            taskFut.get(); //if something went wrong - we'll get exception here
                        }
                        catch (IgniteCheckedException e) {
                            log.error("Job failed", e);

                            jobFailed.set(true);
                        }

                        // Remove complete future from queue to allow other jobs to proceed.
                        resQueue.remove(taskFut);

                        if (inputExhausted.get() && resQueue.isEmpty())
                            emptyLatch.countDown();
                    }
                });
            }

            inputExhausted.set(true);

            if (resQueue.isEmpty())
                emptyLatch.countDown();

            // Wait for queue to empty.
            log.info("Waiting for empty queue...");

            boolean failedWait = false;

            if (!emptyLatch.await(AWAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                info(">>> Failed to wait for queue to empty.");

                failedWait = true;
            }

            if (!failedWait)
                assertFalse("One or more jobs have failed.", jobFailed.get());

            Collection<Integer> absentKeys = findAbsentKeys(runningWorkers.get(0), testKeys);

            if (!failedWait && !absentKeys.isEmpty()) {
                // Give some time to preloader.
                U.sleep(15000);

                absentKeys = findAbsentKeys(runningWorkers.get(0), testKeys);
            }

            info(">>> Absent keys: " + absentKeys);

            assertTrue(absentKeys.isEmpty());

            // Actual primary cache size.
            int primaryCacheSize = 0;

            for (Ignite g : runningWorkers) {
                info(">>>>> " + g.cache(CACHE_NAME).size());

                primaryCacheSize += g.cache(CACHE_NAME).primarySize();
            }

            assertEquals(TEST_MAP_SIZE, primaryCacheSize);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tries to find keys, that are absent in cache.
     *
     * @param workerNode Worker node.
     * @param keys Keys that are suspected to be absent
     * @return List of absent keys. If no keys are absent, the list is empty.
     * @throws IgniteCheckedException If error occurs.
     */
    private Collection<Integer> findAbsentKeys(Ignite workerNode,
        Collection<Integer> keys) throws IgniteCheckedException {

        Collection<Integer> ret = new ArrayList<>(keys.size());

        GridCache<Object, Object> cache = workerNode.cache(CACHE_NAME);

        for (Integer key : keys) {
            if (cache.get(key) == null) // Key is absent.
                ret.add(key);
        }

        return ret;
    }

    /**
     * Generates a test keys collection.
     *
     * @return A test keys collection.
     */
    private Collection<Integer> generateTestKeys() {
        Collection<Integer> ret = new ArrayList<>(TEST_MAP_SIZE);

        for (int i = 0; i < TEST_MAP_SIZE; i++)
            ret.add(i);

        return ret;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDeploymentMode(IgniteDeploymentMode.CONTINUOUS);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setAckTimeout(60000);
        discoverySpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoverySpi);

        if (gridName.startsWith("master")) {
            cfg.setUserAttributes(ImmutableMap.of("segment", "master"));

            // For sure.
            failoverSpi.setMaximumFailoverAttempts(50);

            cfg.setFailoverSpi(failoverSpi);
        }
        else if (gridName.startsWith("worker")) {
            cfg.setUserAttributes(ImmutableMap.of("segment", "worker"));

            GridCacheConfiguration cacheCfg = defaultCacheConfiguration();
            cacheCfg.setName("partitioned");
            cacheCfg.setCacheMode(GridCacheMode.PARTITIONED);
            cacheCfg.setStartSize(4500000);

            cacheCfg.setBackups(backups);

            cacheCfg.setStoreValueBytes(true);
            cacheCfg.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
            cacheCfg.setQueryIndexEnabled(false);

            cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(cacheCfg);
        }
        else
            throw new IllegalStateException("Unexpected grid name: " + gridName);

        return cfg;
    }

    /**
     * Test failover SPI for master node.
     */
    @IgniteSpiConsistencyChecked(optional = true)
    private static class MasterFailoverSpi extends AlwaysFailoverSpi {
        /** */
        private static final String FAILOVER_NUMBER_ATTR = "failover:number:attr";

        /** */
        private Set<ComputeJobContext> failedOverJobs = new HashSet<>();

        /** Node filter. */
        private IgnitePredicate<? super ClusterNode>[] filter;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /**
         * @param filter Filter.
         */
        MasterFailoverSpi(IgnitePredicate<? super ClusterNode>... filter) {
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> top) {
            failedOverJobs.add(ctx.getJobResult().getJobContext());

            // Clear failed nodes list - allow to failover on the same node.
            ctx.getJobResult().getJobContext().setAttribute(FAILED_NODE_LIST_ATTR, null);

            // Account for maximum number of failover attempts since we clear failed node list.
            Integer failoverCnt = ctx.getJobResult().getJobContext().getAttribute(FAILOVER_NUMBER_ATTR);

            if (failoverCnt == null)
                ctx.getJobResult().getJobContext().setAttribute(FAILOVER_NUMBER_ATTR, 1);
            else {
                if (failoverCnt >= getMaximumFailoverAttempts()) {
                    U.warn(log, "Job failover failed because number of maximum failover attempts is exceeded " +
                        "[failedJob=" + ctx.getJobResult().getJob() + ", maxFailoverAttempts=" +
                        getMaximumFailoverAttempts() + ']');

                    return null;
                }

                ctx.getJobResult().getJobContext().setAttribute(FAILOVER_NUMBER_ATTR, failoverCnt + 1);
            }

            List<ClusterNode> cp = new ArrayList<>(top);

            // Keep collection type.
            F.retain(cp, false, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return F.isAll(node, filter);
                }
            });

            return super.failover(ctx, cp); //use cp to ensure we don't failover on failed node
        }

        /**
         * @return Job contexts for failed over jobs.
         */
        public Set<ComputeJobContext> getFailedOverJobs() {
            return failedOverJobs;
        }
    }
}
