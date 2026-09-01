package org.apache.ignite.internal.commandline.snapshot;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_PARTS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/** */
public class SnapshotStatusReproducerTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCommunicationSpi(new TestCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        autoConfirmation = false;

        cleanPersistenceDir();

        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        IntStream.range(0, 2048).forEach(i -> cache.put(i, i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void test() throws Exception {
        injectTestSystemOut();

        IgniteSnapshotManager snapshotMgr = (IgniteSnapshotManager)grid(0).snapshot();

        snapshotMgr.createSnapshot("test_snapshot").get(getTestTimeout());

        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        var checkSingleResultsReceivedLatch = new CountDownLatch(2);
        var restoreSingleResultsReceivedLatch = new AtomicInteger(2);
        var proceedCheckLatch = new CountDownLatch(1);

        for (var ig : Arrays.asList(grid(1), grid(2))) {
            ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).msgCsmr = msg -> {
                if (!(msg instanceof GridIoMessage ioMsg))
                    return;

                if (!(ioMsg.message() instanceof SingleNodeMessage<?> sm))
                    return;

                if (sm.type() == RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE.ordinal())
                    restoreSingleResultsReceivedLatch.decrementAndGet();
                else if (sm.type() == CHECK_SNAPSHOT_PARTS.ordinal()) {
                    checkSingleResultsReceivedLatch.countDown();

                    try {
                        assertTrue(proceedCheckLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        IgniteFutureImpl<Void> restoreFut = snapshotMgr.restoreSnapshot("test_snapshot", null, null, 0, true);

        assertTrue(checkSingleResultsReceivedLatch.await(getTestTimeout(), MILLISECONDS));

        // Make sure no restoration started or finished.
        assertTrue(restoreSingleResultsReceivedLatch.get() > 0);
        assertFalse("Snapshot future has finished", restoreFut.isDone());

        int code = execute("--snapshot", "status");

        // Ensures that there is a status despite unstarted restore process.
        assertEquals("Unexpected exit code", EXIT_CODE_OK, code);

        var out = testOut.toString();

        assertContains(log, out, "Check snapshot operation is in progress");
        assertContains(log, out, "Snapshot name: test_snapshot");
        assertContains(log, out, "Incremental: false");
        assertContains(log, out, "Estimated operation progress:");

        proceedCheckLatch.countDown();

        // Wait for future to finish in order to avoid excessive message about task cancellation.
        restoreFut.get();

        assertTrue(restoreSingleResultsReceivedLatch.get() == 0);
    }

    /** */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile @Nullable Consumer<Message> msgCsmr;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            var msgCsmr = this.msgCsmr;

            if (msgCsmr != null)
                msgCsmr.accept(msg);

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            var msgCsmr = this.msgCsmr;

            if (msgCsmr != null)
                msgCsmr.accept(msg);

            super.sendMessage(node, msg, ackC);
        }
    }
}
