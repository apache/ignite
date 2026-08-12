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

package org.apache.ignite.internal.util.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;

/**
 * Tests that a result of {@link DistributedProcess} is never marshalled on a discovery thread. Marshalling registers
 * unknown class names in the cluster, which takes a discovery round, and a discovery thread cannot wait for a round it
 * has to carry itself.
 */
public class DistributedProcessMarshalThreadTest extends GridCommonAbstractTest {
    /** Timeout to wait latches. */
    private static final long TIMEOUT = 20_000L;

    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Thread that has sent the single node message, and so the one that has marshalled the result. */
    private static final AtomicReference<Thread> SND_THREAD = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // A message is marshalled by the sending thread right before the SPI call, so the sender is the marshaller.
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof SingleNodeMessage)
                    SND_THREAD.compareAndSet(null, Thread.currentThread());

                super.sendMessage(node, msg, ackC);
            }
        });

        cfg.setPluginProviders(new MessagesPluginProvider(TestIntegerMessage.class, TestUuidMessage.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        SND_THREAD.set(null);

        super.afterTest();
    }

    /**
     * Starts a process whose local step finishes synchronously, so that the result is ready while the discovery
     * thread is still inside the init message listener.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResultIsNotMarshalledOnDiscoveryThread() throws Exception {
        startGrids(NODES_CNT);

        CountDownLatch finishLatch = new CountDownLatch(NODES_CNT);

        Map<String, DistributedProcess<TestIntegerMessage, TestUuidMessage>> processes = new HashMap<>();

        for (Ignite grid : G.allGrids()) {
            DistributedProcess<TestIntegerMessage, TestUuidMessage> p = new DistributedProcess<>(
                ((IgniteEx)grid).context(),
                TEST_PROCESS,
                // An already finished future makes DistributedProcess send the result inline, on the caller thread.
                (uuid, req) -> new GridFinishedFuture<>(new TestUuidMessage(UUID.randomUUID())),
                (uuid, res, err) -> finishLatch.countDown()
            );

            processes.put(grid.name(), p);
        }

        processes.get(grid(0).name()).start(UUID.randomUUID(), new TestIntegerMessage(1));

        assertTrue("The process has not finished", finishLatch.await(TIMEOUT, MILLISECONDS));

        Thread sndThread = SND_THREAD.get();

        assertNotNull("No single node message was sent, the test proves nothing", sndThread);

        assertFalse("The result was marshalled on a discovery thread: " + sndThread.getName(),
            sndThread instanceof IgniteDiscoveryThread);
    }
}
