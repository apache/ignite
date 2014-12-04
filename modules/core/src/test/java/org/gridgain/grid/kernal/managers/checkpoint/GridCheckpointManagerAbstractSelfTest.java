/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.checkpoint;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.checkpoint.cache.*;
import org.gridgain.grid.spi.checkpoint.jdbc.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.hsqldb.jdbc.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.compute.GridComputeTaskSessionScope.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class GridCheckpointManagerAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static volatile CountDownLatch startLatch;

    /** */
    private static volatile CountDownLatch read1Latch;

    /** */
    private static volatile CountDownLatch read1FinishedLatch;

    /** */
    private static volatile CountDownLatch read2Latch;

    /** */
    private static volatile CountDownLatch read2FinishedLatch;

    /** */
    private static volatile CountDownLatch read3Latch;

    /** */
    private static volatile CountDownLatch read3FinishedLatch;

    /** */
    private static volatile CountDownLatch rmvLatch;

    /** */
    private static final String GLOBAL_KEY = "test-checkpoint-globalKey";

    /** */
    private static final String GLOBAL_VAL = "test-checkpoint-globalVal";

    /** */
    private static final String GLOBAL_VAL_OVERWRITTEN = GLOBAL_VAL + "-overwritten";

    /** */
    private static final String SES_KEY = "test-checkpoint-sesKey";

    /** */
    private static final String SES_VAL = "test-checkpoint-sesVal";

    /** */
    private static final String SES_VAL_OVERWRITTEN = SES_VAL + "-overwritten";

    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /**
     * Static variable to control whether test should retry checkpoint read attempts.
     * It is needed for s3-based tests because of weak s3 consistency model.
     */
    @SuppressWarnings("RedundantFieldInitialization")
    protected static int retries = 0;

    /**
     * Returns checkpoint manager instance for given Grid.
     *
     * @param ignite Grid instance.
     * @return Checkpoint manager.
     */
    private GridCheckpointManager checkpoints(Ignite ignite) {
        assert ignite != null;

        return ((GridKernal) ignite).context().checkpoint();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        assert gridName != null;

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        if (gridName.contains("cache")) {
            String cacheName = "test-checkpoints";

            GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName(cacheName);
            cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

            GridCacheCheckpointSpi spi = new GridCacheCheckpointSpi();

            spi.setCacheName(cacheName);

            cfg.setCacheConfiguration(cacheCfg);

            cfg.setCheckpointSpi(spi);
        }
        else if (gridName.contains("jdbc")) {
            GridJdbcCheckpointSpi spi = new GridJdbcCheckpointSpi();

            jdbcDataSource ds = new jdbcDataSource();

            ds.setDatabase("jdbc:hsqldb:mem:gg_test_" + getClass().getSimpleName());
            ds.setUser("sa");
            ds.setPassword("");

            spi.setDataSource(ds);
            spi.setCheckpointTableName("test_checkpoints");
            spi.setKeyFieldName("key");
            spi.setValueFieldName("value");
            spi.setValueFieldType("longvarbinary");
            spi.setExpireDateFieldName("expire_date");

            cfg.setCheckpointSpi(spi);
        }

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @throws Exception If test failed.
     */
    protected void doTest(String gridName) throws Exception {
        final AtomicInteger savedCnt = new AtomicInteger();
        final AtomicInteger loadedCnt = new AtomicInteger();
        final AtomicInteger rmvCnt = new AtomicInteger();

        try {
            Ignite ignite = startGrid(gridName);

            ignite.events().localListen(new IgnitePredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    assert evt instanceof GridCheckpointEvent;

                    GridCheckpointEvent e = (GridCheckpointEvent) evt;

                    info("Checkpoint event: " + e);

                    switch (evt.type()) {
                        case EVT_CHECKPOINT_SAVED: {
                            savedCnt.incrementAndGet();

                            break;
                        }

                        case EVT_CHECKPOINT_LOADED: {
                            loadedCnt.incrementAndGet();

                            break;
                        }

                        case EVT_CHECKPOINT_REMOVED: {
                            rmvCnt.incrementAndGet();

                            break;
                        }
                    }

                    return true;
                }
            }, EVT_CHECKPOINT_SAVED, EVT_CHECKPOINT_LOADED, EVT_CHECKPOINT_REMOVED);

            executeAsync(ignite.compute(), GridTestCheckpointTask.class, null).get(2 * 60 * 1000);

            assert checkCheckpointManager(ignite) : "Session IDs got stuck after task completion: " +
                checkpoints(ignite).sessionIds();
        }
        finally {
            stopGrid(gridName);
        }

        assertEquals(8, savedCnt.get());
        assertEquals(10, loadedCnt.get());

        if ("jdbc".equals(gridName))
            assertEquals(5, rmvCnt.get());
        else
            assertEquals(6, rmvCnt.get());
    }

    /**
     * @param gridName Grid name.
     * @throws Exception If test failed.
     */
    protected void doMultiNodeTest(String gridName) throws Exception {
        startLatch = new CountDownLatch(3);

        read1Latch = new CountDownLatch(1);
        read1FinishedLatch = new CountDownLatch(2);

        read2Latch = new CountDownLatch(1);
        read2FinishedLatch = new CountDownLatch(2);

        read3Latch = new CountDownLatch(1);
        read3FinishedLatch = new CountDownLatch(2);

        rmvLatch = new CountDownLatch(1);

        try {
            startGrid(gridName + 1);

            Ignite ignite = startGrid(gridName);

            IgniteFuture fut = executeAsync(ignite.compute(), new GridMultiNodeGlobalConsumerTask(), null);

            executeAsync(ignite.compute(), GridMultiNodeTestCheckPointTask.class, null).get(2 * 60 * 1000);

            fut.get();

            for (Ignite g : G.allGrids()) {
                assert checkCheckpointManager(g) : "Session IDs got stuck after task completion [grid=" + g.name() +
                    ", sesIds=" + checkpoints(g).sessionIds() + ']';
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param g Grid.
     * @return {@code True} if checkpoint manager is OK.
     * @throws Exception If failed.
     */
    @SuppressWarnings( {"BusyWait"})
    private boolean checkCheckpointManager(Ignite g) throws Exception {
        int i = 0;

        while (true) {
            Collection<IgniteUuid> sesIds = checkpoints(g).sessionIds();

            if (sesIds.isEmpty())
                return true;

            if (++i == 3)
                return false;

            Thread.sleep(1000);
        }
    }

    /**
     * Test job.
     */
    private static class GridTestCheckpointJob extends GridComputeJobAdapter {
        /** */
        @GridInstanceResource
        private Ignite ignite;

        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @SuppressWarnings({"TooBroadScope"})
        @Override public String execute() throws GridException {
            assert ignite != null;
            assert taskSes != null;

            final String key1 = "test-checkpoint-key1";
            final String val1 = "test-checkpoint-value1";

            final String key2 = "test-checkpoint-key2";
            final String val2 = "test-checkpoint-value2";

            String key3 = "test-checkpoint-key3";
            String val3 = "test-checkpoint-value3";

            taskSes.saveCheckpoint(key1, val1, GLOBAL_SCOPE, 0);
            taskSes.saveCheckpoint(key2, val2, SESSION_SCOPE, 0);

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert val1.equals(taskSes.loadCheckpoint(key1));
                    assert val2.equals(taskSes.loadCheckpoint(key2));
                }
            });

            // Don't overwrite.
            taskSes.saveCheckpoint(key1, val2, GLOBAL_SCOPE, 0, false);
            taskSes.saveCheckpoint(key2, val1, SESSION_SCOPE, 0, false);

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert val1.equals(taskSes.loadCheckpoint(key1));
                    assert val2.equals(taskSes.loadCheckpoint(key2));
                }
            });

            taskSes.saveCheckpoint(key1, val2, GLOBAL_SCOPE, 0, true);
            taskSes.saveCheckpoint(key2, val1, SESSION_SCOPE, 0, true);

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert val2.equals(taskSes.loadCheckpoint(key1));
                    assert val1.equals(taskSes.loadCheckpoint(key2));
                }
            });

            assert taskSes.removeCheckpoint(key1);
            assert taskSes.removeCheckpoint(key2);
            assert !taskSes.removeCheckpoint(key1);
            assert !taskSes.removeCheckpoint(key2);

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert taskSes.loadCheckpoint(key1) == null;
                    assert taskSes.loadCheckpoint(key2) == null;
                }
            });

            taskSes.saveCheckpoint(key1, val1, GLOBAL_SCOPE, 0);

            ((IgniteMBean) ignite).removeCheckpoint(key1);

            // This checkpoint will not be automatically removed for cache SPI.
            taskSes.saveCheckpoint(key1, val1, GLOBAL_SCOPE, 5000);

            // This will be automatically removed by cache SPI.
            taskSes.saveCheckpoint(key2, val2, SESSION_SCOPE, 5000);

            try {
                Thread.sleep(6000);
            }
            catch (InterruptedException e) {
                throw new GridException(e);
            }

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert taskSes.loadCheckpoint(key1) == null;
                    assert taskSes.loadCheckpoint(key2) == null;
                }
            });

            // This checkpoint will be removed when task session end.
            taskSes.saveCheckpoint(key3, val3, SESSION_SCOPE, 0);

            return null;
        }
    }

    /**
     * Test task.
     */
    @GridComputeTaskSessionFullSupport
    private static class GridTestCheckpointTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            return Collections.singleton(new GridTestCheckpointJob());
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Multi-node test consumer job.
     */
    private static class GridMultiNodeTestCheckpointProducerJob extends GridComputeJobAdapter {
        /** */
        @GridInstanceResource
        private Ignite ignite;

        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override public String execute() throws GridException {
            assert ignite != null;
            assert taskSes != null;

            assert startLatch != null;

            assert read1Latch != null;
            assert read2Latch != null;
            assert read3Latch != null;

            assert read1FinishedLatch != null;
            assert read2FinishedLatch != null;
            assert read3FinishedLatch != null;

            assert rmvLatch != null;

            startLatch.countDown();

            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            X.println(">>> Producer started.");

            taskSes.saveCheckpoint(GLOBAL_KEY, GLOBAL_VAL, GLOBAL_SCOPE, 0);
            taskSes.saveCheckpoint(SES_KEY, SES_VAL, SESSION_SCOPE, 0);

            read1Latch.countDown();

            try {
                read1FinishedLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            // No retries here as other thread should have seen checkpoint already.
            assert GLOBAL_VAL.equals(taskSes.loadCheckpoint(GLOBAL_KEY));
            assert SES_VAL.equals(taskSes.loadCheckpoint(SES_KEY));

            taskSes.saveCheckpoint(GLOBAL_KEY, SES_VAL + "-notoverwritten", GLOBAL_SCOPE, 0, false);
            taskSes.saveCheckpoint(SES_KEY, GLOBAL_VAL + "-notoverwritten", SESSION_SCOPE, 0, false);

            read2Latch.countDown();

            try {
                read2FinishedLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            assert GLOBAL_VAL.equals(taskSes.loadCheckpoint(GLOBAL_KEY));
            assert SES_VAL.equals(taskSes.loadCheckpoint(SES_KEY));

            // Swap values.
            taskSes.saveCheckpoint(GLOBAL_KEY, SES_VAL_OVERWRITTEN, GLOBAL_SCOPE, 0, true);
            taskSes.saveCheckpoint(SES_KEY, GLOBAL_VAL_OVERWRITTEN, SESSION_SCOPE, 0, true);

            read3Latch.countDown();

            try {
                read3FinishedLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            assert SES_VAL_OVERWRITTEN.equals(taskSes.loadCheckpoint(GLOBAL_KEY));
            assert GLOBAL_VAL_OVERWRITTEN.equals(taskSes.loadCheckpoint(SES_KEY));

            // Remove checkpoints.
            assert taskSes.removeCheckpoint(GLOBAL_KEY);
            assert taskSes.removeCheckpoint(SES_KEY);

            // Check checkpoints are actually removed.
            assert !taskSes.removeCheckpoint(GLOBAL_KEY);
            assert !taskSes.removeCheckpoint(SES_KEY);

            rmvLatch.countDown();

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert taskSes.loadCheckpoint(GLOBAL_KEY) == null;
                    assert taskSes.loadCheckpoint(SES_KEY) == null;
                }
            });

            return null;
        }
    }

    /**
     * Multi-node test consumer job.
     */
    private static class GridMultiNodeTestCheckpointConsumerJob extends GridComputeJobAdapter {
        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override public String execute() throws GridException {
            assert taskSes != null;

            assert startLatch != null;

            assert read1Latch != null;
            assert read2Latch != null;
            assert read3Latch != null;

            assert read1FinishedLatch != null;
            assert read2FinishedLatch != null;
            assert read3FinishedLatch != null;

            assert rmvLatch != null;

            startLatch.countDown();

            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            X.println(">>> Consumer started.");

            try {
                read1Latch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            // Test that checkpoints were saved properly.
            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert GLOBAL_VAL.equals(taskSes.loadCheckpoint(GLOBAL_KEY));
                    assert SES_VAL.equals(taskSes.loadCheckpoint(SES_KEY));
                }
            });

            read1FinishedLatch.countDown();

            try {
                read2Latch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            // Test that checkpoints were not overwritten.
            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert GLOBAL_VAL.equals(taskSes.loadCheckpoint(GLOBAL_KEY));
                    assert SES_VAL.equals(taskSes.loadCheckpoint(SES_KEY));
                }
            });

            read2FinishedLatch.countDown();

            try {
                read3Latch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assertEquals(SES_VAL_OVERWRITTEN, taskSes.loadCheckpoint(GLOBAL_KEY));
                    assertEquals(GLOBAL_VAL_OVERWRITTEN, taskSes.loadCheckpoint(SES_KEY));
                }
            });

            read3FinishedLatch.countDown();

            try {
                rmvLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Thread has been interrupted.", e);
            }
            // Check checkpoints are actually removed.
            assert !taskSes.removeCheckpoint(GLOBAL_KEY);
            assert !taskSes.removeCheckpoint(SES_KEY);

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws GridException {
                    assert taskSes.loadCheckpoint(GLOBAL_KEY) == null;
                    assert taskSes.loadCheckpoint(SES_KEY) == null;
                }
            });

            return null;
        }
    }

    /**
     * Multi-node test task.
     */
    @GridComputeTaskSessionFullSupport
    private static class GridMultiNodeTestCheckPointTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            assert gridSize == 2;

            return Arrays.asList(
                new GridMultiNodeTestCheckpointProducerJob(),
                new GridMultiNodeTestCheckpointConsumerJob()
            );
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /** */
    @GridComputeTaskSessionFullSupport
    private static class GridMultiNodeGlobalConsumerTask extends GridComputeTaskSplitAdapter<Object, Integer> {
        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            return Collections.singleton(new GridComputeJobAdapter() {
                @Nullable @Override public Object execute() throws GridException {
                    assert taskSes != null;

                    assert startLatch != null;

                    assert read1Latch != null;
                    assert read2Latch != null;
                    assert read3Latch != null;

                    assert read1FinishedLatch != null;
                    assert read2FinishedLatch != null;
                    assert read3FinishedLatch != null;

                    assert rmvLatch != null;

                    startLatch.countDown();

                    try {
                        startLatch.await();
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Thread has been interrupted.", e);
                    }

                    X.println(">>> Global consumer started.");

                    try {
                        read1Latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Thread has been interrupted.", e);
                    }

                    // Test that checkpoints were saved properly.
                    assert GLOBAL_VAL.equals(taskSes.loadCheckpoint(GLOBAL_KEY));
                    assert SES_VAL.equals(taskSes.loadCheckpoint(SES_KEY));

                    read1FinishedLatch.countDown();

                    try {
                        read2Latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Thread has been interrupted.", e);
                    }

                    // Test that checkpoints were not overwritten.
                    assert GLOBAL_VAL.equals(taskSes.loadCheckpoint(GLOBAL_KEY));
                    assert SES_VAL.equals(taskSes.loadCheckpoint(SES_KEY));

                    read2FinishedLatch.countDown();

                    try {
                        read3Latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Thread has been interrupted.", e);
                    }

                    assert GLOBAL_VAL_OVERWRITTEN.equals(taskSes.loadCheckpoint(SES_KEY));
                    assert SES_VAL_OVERWRITTEN.equals(taskSes.loadCheckpoint(GLOBAL_KEY));

                    read3FinishedLatch.countDown();

                    try {
                        rmvLatch.await();
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Thread has been interrupted.", e);
                    }
                    // Check checkpoints are actually removed.
                    assert !taskSes.removeCheckpoint(GLOBAL_KEY);
                    assert !taskSes.removeCheckpoint(SES_KEY);

                    assert taskSes.loadCheckpoint(GLOBAL_KEY) == null;
                    assert taskSes.loadCheckpoint(SES_KEY) == null;

                    return 0;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            int sum = 0;

            for (GridComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }

    /**
     * Wrapper around {@link GridTestUtils#retryAssert(GridLogger, int, long, GridAbsClosure)}.
     * For the given closure provides count of retries, configured by {@link #retries} attribute.
     * @param assertion Closure with assertion inside.
     * @throws GridInterruptedException If was interrupted.
     */
    private static void assertWithRetries(GridAbsClosureX assertion) throws GridInterruptedException {
        GridTestUtils.retryAssert(null, retries, 5000, assertion);
    }
}
