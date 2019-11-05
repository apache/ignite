package org.apache.ignite.internal.encryption;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.DistributedProcessActionMessage;
import org.apache.ignite.internal.util.distributed.DistributedProcessInitialMessage;
import org.apache.ignite.internal.util.distributed.DistributedProcessSingleNodeMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests {@link DistributedProcess}.
 */
public class DistributedProcessTest extends GridCommonAbstractTest {
    static {
        GridIoMessageFactory.registerCustom(TestSingleNodeResult.DIRECT_TYPE, (CO<Message>)TestSingleNodeResult::new);
    }

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

        ArrayList<TestDistributedProcess> processes = new ArrayList<>();

        G.allGrids().forEach(ignite -> {
            TestDistributedProcess process = new TestDistributedProcess();

            process.init(((IgniteEx)ignite).context(), TestRequest.class,
                TestSingleNodeResult.class, GridTopic.TOPIC_MASTER_KEY_CHANGE, TestResult.class);

            processes.add(process);
        });

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(2));

        spi.blockMessages((node, msg) -> msg instanceof TestSingleNodeResult);

        crd.context().discovery().sendCustomEvent(new TestRequest(UUID.randomUUID()));

        spi.waitForBlocked();

        processes.forEach(process -> assertEquals(1, process.completed.getCount()));

        spi.stopBlock(false, null, false, true);

        stopGrid(0);
        processes.remove(0);

        awaitPartitionMapExchange();

        processes.forEach(process -> assertEquals(1, process.completed.getCount()));

        spi.waitForBlocked();

        processes.forEach(process -> assertEquals(1, process.completed.getCount()));

        spi.stopBlock();

        processes.forEach(process -> {
            try {
                process.completed.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        });
    }

    /** Test implementation of {@link DistributedProcess}. */
    private static class TestDistributedProcess extends DistributedProcess<TestRequest, TestSingleNodeResult, TestResult> {
        CountDownLatch completed = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override protected IgniteInternalFuture<TestSingleNodeResult> process(TestRequest msg,
            AffinityTopologyVersion topVer) {
            return new GridFinishedFuture<>(new TestSingleNodeResult(msg.requestId()));
        }

        /** {@inheritDoc} */
        @Override protected void onAllReceived(Map<UUID, TestSingleNodeResult> res) {
            sendAction(new TestResult(res.values().stream().findFirst().get().requestId()));
        }

        /** {@inheritDoc} */
        @Override protected void onActionMessage(TestResult msg) {
            completed.countDown();
        }
    }

    /** Test implementation of {@link DistributedProcessInitialMessage}. */
    private static class TestRequest implements DistributedProcessInitialMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** Custom message ID. */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** Request id. */
        private final UUID reqId;

        /** @param id Request id. */
        TestRequest(UUID id) {
            reqId = id;
        }

        /** {@inheritDoc} */
        @Override public UUID requestId() {
            return reqId;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            return null;
        }
    }

    /** Test implementation of {@link DistributedProcessSingleNodeMessage}. */
    private static class TestSingleNodeResult implements DistributedProcessSingleNodeMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public static final short DIRECT_TYPE = 205;

        /** Request id. */
        private UUID reqId;

        /** */
        public TestSingleNodeResult() {
        }

        /** @param reqId Request id. */
        public TestSingleNodeResult(UUID reqId) {
            this.reqId = reqId;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(directType(), fieldsCount()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeUuid("reqId", reqId))
                        return false;

                    writer.incrementState();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            reader.setBuffer(buf);

            if (!reader.beforeMessageRead())
                return false;

            switch (reader.state()) {
                case 0:
                    reqId = reader.readUuid("reqId");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return reader.afterMessageRead(TestSingleNodeResult.class);
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return DIRECT_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public UUID requestId() {
            return reqId;
        }
    }

    /** Test implementation of {@link DistributedProcessActionMessage}. */
    private static class TestResult implements DistributedProcessActionMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** Custom message ID. */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** Request id. */
        private final UUID reqId;

        /** @param id Request id. */
        TestResult(UUID id) {
            reqId = id;
        }

        /** {@inheritDoc} */
        @Override public UUID requestId() {
            return reqId;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            return null;
        }
    }

}
