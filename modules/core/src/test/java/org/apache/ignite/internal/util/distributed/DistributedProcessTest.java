package org.apache.ignite.internal.util.distributed;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcesses.TEST_PROCESS;

/**
 * Tests {@link DistributedProcessManager}.
 */
public class DistributedProcessTest extends GridCommonAbstractTest {
    /** */
    private final ConcurrentHashMap<UUID, TestDistributedProcess> processes = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testCoordinatorChange() throws Exception {
        IgniteEx crd = startGrids(3);

        AtomicReference<DistributedProcessManager> mgr = new AtomicReference<>();

        G.allGrids().forEach(ignite -> {
            GridKernalContext ctx = ((IgniteEx)ignite).context();

            DistributedProcessManager mgr0 = U.field(ctx.encryption(), "dpMgr");

            mgr0.register(TEST_PROCESS, new TestDistributedProcessFactory(ctx.localNodeId()));

            mgr.compareAndSet(null, mgr0);
        });

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(2));

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        mgr.get().start(TEST_PROCESS, 10);

        spi.waitForBlocked();

        processes.forEach((uuid, process) -> assertEquals(1, process.completed.getCount()));
        processes.forEach((uuid, process) -> assertEquals(0, process.result.get()));

        spi.stopBlock(false, null, false, true);

        processes.remove(crd.localNode().id());

        stopGrid(0);

        awaitPartitionMapExchange();

        processes.forEach((uuid, process) -> assertEquals(1, process.completed.getCount()));
        processes.forEach((uuid, process) -> assertEquals(0, process.result.get()));

        spi.waitForBlocked();

        processes.forEach((uuid, process) -> assertEquals(1, process.completed.getCount()));
        processes.forEach((uuid, process) -> assertEquals(0, process.result.get()));

        spi.stopBlock();

        processes.forEach((uuid, process) -> {
            try {
                process.completed.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        });

        processes.forEach((uuid, process) -> assertEquals(20, process.result.get()));
    }

    /** Test implementation of {@link DistributedProcessManager}. */
    private class TestDistributedProcess implements DistributedProcess<Integer, Integer, Integer> {
        /** */
        private final CountDownLatch completed = new CountDownLatch(1);

        /** */
        private final AtomicLong result = new AtomicLong();

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<Integer> execute(Integer req) {
            return new GridFinishedFuture<>(req);
        }

        /** {@inheritDoc} */
        @Override public Integer buildResult(Map<UUID, Integer> map) {
            return map.values().stream().mapToInt(value -> value).sum();
        }

        /** {@inheritDoc} */
        @Override public void finish(Integer res) {
            completed.countDown();

            result.addAndGet(res);
        }

        /** {@inheritDoc} */
        @Override public void cancel(Exception e) {
            // No-op.
        }
    }

    /** */
    private class TestDistributedProcessFactory implements DistributedProcessFactory {
        /** Node id. */
        private final UUID nodeId;

        /** @param nodeId Node id. */
        public TestDistributedProcessFactory(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public DistributedProcess create() {
            TestDistributedProcess p = new TestDistributedProcess();

            processes.put(nodeId, p);

            return p;
        }
    }
}
