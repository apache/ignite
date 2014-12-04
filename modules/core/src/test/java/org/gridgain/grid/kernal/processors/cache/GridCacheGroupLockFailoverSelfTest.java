/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import com.google.common.collect.*;
import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests group lock transaction failover.
 */
public class GridCacheGroupLockFailoverSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Size of the test map. */
    private static final int TEST_MAP_SIZE = 200000;

    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Size of data chunk, sent to a remote node. */
    private static final int DATA_CHUNK_SIZE = 1000;

    /** Number of chunk on which to fail worker node. */
    public static final int FAIL_ON_CHUNK_NO = (TEST_MAP_SIZE / DATA_CHUNK_SIZE) / 3;

    /** */
    private static final int FAILOVER_PUSH_GAP = 30;

    /** Master node name. */
    private static final String MASTER = "master";

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** Backups count. */
    private int backups;

    /** Filter to include only worker nodes. */
    private static final GridPredicate<ClusterNode> workerNodesFilter = new PN() {
        @SuppressWarnings("unchecked")
        @Override public boolean apply(ClusterNode n) {
            return "worker".equals(n.attribute("segment"));
        }
    };

    /**
     * Result future queue (restrict the queue size
     * to 50 in order to prevent in-memory data grid from over loading).
     */
    private final BlockingQueue<GridComputeTaskFuture<?>> resQueue = new LinkedBlockingQueue<>(10);

    /**
     * @return {@code True} if test should use optimistic transactions.
     */
    protected boolean optimisticTx() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverGroupLockNearEnabledOneBackup() throws Exception {
        checkPutAllFailoverGroupLock(true, 3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverGroupLockNearDisabledOneBackup() throws Exception {
        checkPutAllFailoverGroupLock(false, 3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverGroupLockNearEnabledTwoBackups() throws Exception {
        checkPutAllFailoverGroupLock(true, 5, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverGroupLockNearDisabledTwoBackups() throws Exception {
        checkPutAllFailoverGroupLock(false, 5, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverGroupLockNearEnabledThreeBackups() throws Exception {
        checkPutAllFailoverGroupLock(true, 7, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverGroupLockNearDisabledThreeBackups() throws Exception {
        checkPutAllFailoverGroupLock(false, 7, 3);
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
     * @param near {@code True} for near enabled.
     * @param workerCnt Workers count.
     * @param shutdownCnt Shutdown count.
     * @throws Exception If failed.
     */
    public void checkPutAllFailoverGroupLock(boolean near, int workerCnt, int shutdownCnt) throws Exception {
        nearEnabled = near;
        backups = shutdownCnt;

        Collection<Integer> testKeys = generateTestKeys();

        Ignite master = startGrid(MASTER);

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

            int failoverPushGap = 0;

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

                    info("Pushing data chunk: " + chunkCntr);

                    submitDataChunk(master, nodeId, data);

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

            // Submit the rest of data.
            for (Map.Entry<UUID, Collection<Integer>> entry : dataChunks.entrySet())
                submitDataChunk(master, entry.getKey(), entry.getValue());

            // Wait for queue to empty.
            info("Waiting for empty queue...");

            long seenSize = resQueue.size();

            while (true) {
                U.sleep(10000);

                if (!resQueue.isEmpty()) {
                    long size = resQueue.size();

                    if (seenSize == size) {
                        info(">>> Failed to wait for queue to empty.");

                        break;
                    }

                    seenSize = size;
                }
                else
                    break;
            }

            Collection<Integer> absentKeys = findAbsentKeys(runningWorkers.get(0), testKeys);

            info(">>> Absent keys: " + absentKeys);

            assertTrue(absentKeys.isEmpty());

            // Actual primary cache size.
            int primaryCacheSize = 0;

            for (Ignite g : runningWorkers) {
                info(">>>>> " + g.cache(CACHE_NAME).size());

                primaryCacheSize += g.cache(CACHE_NAME).primarySize();
            }

            assertTrue(TEST_MAP_SIZE <= primaryCacheSize);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Does remapping.
     * @param master Master grid.
     * @param keys Keys.
     * @throws GridException If failed.
     */
    private void remap(final Ignite master, Iterable<Integer> keys) throws GridException {
        Map<UUID, Collection<Integer>> dataChunks = new HashMap<>();

        for (Integer key : keys) {
            ClusterNode mappedNode = master.cluster().mapKeyToNode(CACHE_NAME, key);

            UUID nodeId = mappedNode.id();

            Collection<Integer> data = dataChunks.get(nodeId);

            if (data == null) {
                data = new ArrayList<>(DATA_CHUNK_SIZE);

                dataChunks.put(nodeId, data);
            }

            data.add(key);
        }

        for (Map.Entry<UUID, Collection<Integer>> entry : dataChunks.entrySet())
            submitDataChunk(master, entry.getKey(), entry.getValue());
    }

    /**
     * Submits next data chunk as grid task. Blocks if queue is full.
     *
     * @param master Master node to submit from.
     * @param preferredNodeId Node id to execute job on.
     * @param dataChunk Data chunk to put in cache.
     * @throws GridException If failed.
     */
    private void submitDataChunk(final Ignite master, UUID preferredNodeId, final Collection<Integer> dataChunk)
        throws GridException {
        ClusterGroup prj = master.cluster().forPredicate(workerNodesFilter);

        GridCompute comp = master.compute(prj).enableAsync();

        comp.execute(new GridCacheGroupLockPutTask(preferredNodeId, CACHE_NAME, optimisticTx()), dataChunk);

        GridComputeTaskFuture<Void> fut = comp.future();

        fut.listenAsync(new CI1<GridFuture<Void>>() {
            @Override public void apply(GridFuture<Void> f) {
                GridComputeTaskFuture taskFut = (GridComputeTaskFuture)f;

                boolean fail = false;

                try {
                    f.get(); //if something went wrong - we'll get exception here
                }
                catch (GridException ignore) {
                    info("Put task failed, going to remap keys: " + dataChunk.size());

                    fail = true;
                }
                finally {
                    // Remove complete future from queue to allow other jobs to proceed.
                    resQueue.remove(taskFut);

                    try {
                        if (fail)
                            remap(master, dataChunk);
                    }
                    catch (GridException e) {
                        info("Failed to remap task [data=" + dataChunk.size() + ", e=" + e + ']');
                    }
                }
            }
        });

        try {
            resQueue.put(fut);

            if (fut.isDone())
                resQueue.remove(fut);
        }
        catch (InterruptedException ignored) {
            info(">>>> Failed to wait for future submission: " + fut);

            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tries to find keys, that are absent in cache.
     *
     * @param workerNode Worker node.
     * @param keys Keys that are suspected to be absent
     * @return List of absent keys. If no keys are absent, the list is empty.
     * @throws GridException If error occurs.
     */
    private Collection<Integer> findAbsentKeys(Ignite workerNode,
        Collection<Integer> keys) throws GridException {

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

        cfg.setDeploymentMode(GridDeploymentMode.CONTINUOUS);

        GridTcpDiscoverySpi discoverySpi = new GridTcpDiscoverySpi();

        discoverySpi.setAckTimeout(60000);
        discoverySpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoverySpi);

        if (gridName.startsWith("master")) {
            cfg.setUserAttributes(ImmutableMap.of("segment", "master"));

            GridTestFailoverSpi failoverSpi = new GridTestFailoverSpi(true, (GridPredicate)workerNodesFilter);

            // For sure.
            failoverSpi.setMaximumFailoverAttempts(50);

            cfg.setFailoverSpi(failoverSpi);
        }
        else if (gridName.startsWith("worker")) {
            GridTestFailoverSpi failoverSpi = new GridTestFailoverSpi(false);

            cfg.setFailoverSpi(failoverSpi);

            cfg.setUserAttributes(ImmutableMap.of("segment", "worker"));

            GridCacheConfiguration cacheCfg = defaultCacheConfiguration();
            cacheCfg.setName("partitioned");
            cacheCfg.setCacheMode(GridCacheMode.PARTITIONED);
            cacheCfg.setStartSize(4500000);
            cacheCfg.setBackups(backups);
            cacheCfg.setDgcSuspectLockTimeout(600000);
            cacheCfg.setDgcFrequency(0);
            cacheCfg.setStoreValueBytes(true);
            cacheCfg.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
            cacheCfg.setQueryIndexEnabled(false);
            cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            cfg.setCacheConfiguration(cacheCfg);
        }
        else
            throw new IllegalStateException("Unexpected grid name: " + gridName);

        return cfg;
    }

    /**
     * Test failover SPI that remembers the job contexts of failed jobs.
     */
    private class GridTestFailoverSpi extends GridAlwaysFailoverSpi {
        /** */
        private static final String FAILOVER_NUMBER_ATTR = "failover:number:attr";

        /** */
        private final boolean master;

        /** */
        private Set<GridComputeJobContext> failedOverJobs = new HashSet<>();

        /** Node filter. */
        private GridPredicate<? super ClusterNode>[] filter;

        /**
         * @param master Master flag.
         * @param filter Filters.
         */
        @SafeVarargs
        GridTestFailoverSpi(boolean master, GridPredicate<? super ClusterNode>... filter) {
            this.master = master;
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode failover(GridFailoverContext ctx, List<ClusterNode> top) {
            List<ClusterNode> cp = null;
            if (master) {
                failedOverJobs.add(ctx.getJobResult().getJobContext());

                // Clear failed nodes list - allow to failover on the same node.
                ctx.getJobResult().getJobContext().setAttribute(FAILED_NODE_LIST_ATTR, null);

                // Account for maximum number of failover attempts since we clear failed node list.
                Integer failoverCnt = ctx.getJobResult().getJobContext().getAttribute(FAILOVER_NUMBER_ATTR);

                if (failoverCnt == null)
                    ctx.getJobResult().getJobContext().setAttribute(FAILOVER_NUMBER_ATTR, 1);
                else {
                    if (failoverCnt >= getMaximumFailoverAttempts()) {
                        info("Job failover failed because number of maximum failover attempts is exceeded " +
                            "[failedJob=" + ctx.getJobResult().getJob() + ", maxFailoverAttempts=" +
                            getMaximumFailoverAttempts() + ']');

                        return null;
                    }

                    ctx.getJobResult().getJobContext().setAttribute(FAILOVER_NUMBER_ATTR, failoverCnt + 1);
                }

                cp = new ArrayList<>(top);

                // Keep collection type.
                F.retain(cp, false, new GridPredicate<ClusterNode>() {
                    @Override public boolean apply(ClusterNode node) {
                        return F.isAll(node, filter);
                    }
                });
            }

            return super.failover(ctx, cp); //use cp to ensure we don't failover on failed node
        }

        /**
         * @return Job contexts for failed over jobs.
         */
        public Set<GridComputeJobContext> getFailedOverJobs() {
            return failedOverJobs;
        }
    }
}
