/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tests Hadoop external communication component.
 */
public class GridHadoopExternalCommunicationSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testSimpleMessageSendingTcp() throws Exception {
        checkSimpleMessageSending(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleMessageSendingShmem() throws Exception {
        checkSimpleMessageSending(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSimpleMessageSending(boolean useShmem) throws Exception {
        UUID parentNodeId = UUID.randomUUID();

        GridMarshaller marsh = new GridOptimizedMarshaller();

        IgniteLogger log = log();

        GridHadoopExternalCommunication[] comms = new GridHadoopExternalCommunication[4];

        try {
            String name = "grid";

            TestHadoopListener[] lsnrs = new TestHadoopListener[4];

            int msgs = 10;

            for (int i = 0; i < comms.length; i++) {
                comms[i] = new GridHadoopExternalCommunication(parentNodeId, UUID.randomUUID(), marsh, log,
                    Executors.newFixedThreadPool(1), name + i);

                if (useShmem)
                    comms[i].setSharedMemoryPort(14000);

                lsnrs[i] = new TestHadoopListener(msgs);

                comms[i].setListener(lsnrs[i]);

                comms[i].start();
            }

            for (int r = 0; r < msgs; r++) {
                for (int from = 0; from < comms.length; from++) {
                    for (int to = 0; to < comms.length; to++) {
                        if (from == to)
                            continue;

                        comms[from].sendMessage(comms[to].localProcessDescriptor(), new TestMessage(from, to));
                    }
                }
            }

            U.sleep(1000);

            for (TestHadoopListener lsnr : lsnrs) {
                lsnr.await(3_000);

                assertEquals(String.valueOf(lsnr.messages()), msgs * (comms.length - 1), lsnr.messages().size());
            }
        }
        finally {
            for (GridHadoopExternalCommunication comm : comms) {
                if (comm != null)
                    comm.stop();
            }
        }
    }

    /**
     *
     */
    private static class TestHadoopListener implements GridHadoopMessageListener {
        /** Received messages (array list is safe because executor has one thread). */
        private Collection<TestMessage> msgs = new ArrayList<>();

        /** Await latch. */
        private CountDownLatch receiveLatch;

        /**
         * @param msgs Number of messages to await.
         */
        private TestHadoopListener(int msgs) {
            receiveLatch = new CountDownLatch(msgs);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridHadoopProcessDescriptor desc, GridHadoopMessage msg) {
            assert msg instanceof TestMessage;

            msgs.add((TestMessage)msg);

            receiveLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override public void onConnectionLost(GridHadoopProcessDescriptor desc) {
            // No-op.
        }

        /**
         * @return Received messages.
         */
        public Collection<TestMessage> messages() {
            return msgs;
        }

        /**
         * @param millis Time to await.
         * @throws InterruptedException If wait interrupted.
         */
        public void await(int millis) throws InterruptedException {
            receiveLatch.await(millis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     *
     */
    private static class TestMessage implements GridHadoopMessage {
        /** From index. */
        private int from;

        /** To index. */
        private int to;

        /**
         * @param from From index.
         * @param to To index.
         */
        private TestMessage(int from, int to) {
            this.from = from;
            this.to = to;
        }

        /**
         * Required by {@link Externalizable}.
         */
        public TestMessage() {
            // No-op.
        }

        /**
         * @return From index.
         */
        public int from() {
            return from;
        }

        /**
         * @return To index.
         */
        public int to() {
            return to;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(from);
            out.writeInt(to);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            from = in.readInt();
            to = in.readInt();
        }
    }
}
