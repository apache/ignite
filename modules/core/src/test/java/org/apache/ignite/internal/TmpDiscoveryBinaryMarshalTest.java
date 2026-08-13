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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.managers.communication.MessageMarshalling;
import org.apache.ignite.internal.processors.marshaller.MappingAcceptedMessage;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Temporary probe: binary marshalling of an unknown user type from a discovery thread of a node already in topology. */
public class TmpDiscoveryBinaryMarshalTest extends GridCommonAbstractTest {
    /** */
    private final CountDownLatch lsnrEntered = new CountDownLatch(1);

    /** */
    private final CountDownLatch lsnrDone = new CountDownLatch(1);

    /** */
    private final AtomicReference<String> lsnrThread = new AtomicReference<>();

    /** */
    private final AtomicReference<Object> lsnrRes = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new MessagesPluginProvider(TmpTriggerDiscoveryMessage.class, TmpPayloadMessage.class))
            // Keep the node alive when a system-critical thread blocks, so the stacks can be dumped.
            .setFailureHandler(new NoOpFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        super.afterTest();
    }

    /** */
    @Test
    public void testBinaryMarshalOfUnknownTypeOnDiscoveryThread() throws Exception {
        startGrids(2);

        IgniteEx crd = grid(0);
        IgniteEx node = grid(1);

        GridKernalContext kctx = node.context();

        log.info(">>>> marshaller of the node = " + kctx.marshaller().getClass().getName());

        // Control run: exactly the same call, but from a plain user thread.
        TmpPayloadMessage ctrlMsg = new TmpPayloadMessage(new ControlType(1));

        long start = System.currentTimeMillis();

        MessageMarshalling.marshal(ctrlMsg, kctx.marshaller(), kctx, null);

        log.info(">>>> CONTROL (user thread): marshalled in " + (System.currentTimeMillis() - start) +
            " ms, bytes=" + (ctrlMsg.dataBytes() == null ? -1 : ctrlMsg.dataBytes().length));

        // Trace the mapping exchange on both nodes.
        for (IgniteEx g : new IgniteEx[] {crd, node}) {
            String n = g.name();

            g.context().discovery().setCustomEventListener(MappingProposedMessage.class,
                (topVer, snd, msg) -> log.info(">>>> [" + n + "] MappingProposedMessage seen: " + msg +
                    ", thread=" + Thread.currentThread().getName()));

            g.context().discovery().setCustomEventListener(MappingAcceptedMessage.class,
                (topVer, snd, msg) -> log.info(">>>> [" + n + "] MappingAcceptedMessage seen: " + msg.getMappingItem() +
                    ", thread=" + Thread.currentThread().getName()));
        }

        // The node that is already in topology marshals an unknown type right on the discovery thread.
        node.context().discovery().setCustomEventListener(TmpTriggerDiscoveryMessage.class, (topVer, snd, msg) -> {
            lsnrThread.set(Thread.currentThread().getName());

            lsnrEntered.countDown();

            try {
                TmpPayloadMessage payload = new TmpPayloadMessage(new TmpUnknownUserType(42));

                MessageMarshalling.marshal(payload, kctx.marshaller(), kctx, null);

                lsnrRes.set("OK, bytes=" + (payload.dataBytes() == null ? -1 : payload.dataBytes().length));
            }
            catch (Throwable t) {
                lsnrRes.set(t);
            }
            finally {
                lsnrDone.countDown();
            }
        });

        crd.context().discovery().sendCustomEvent(new TmpTriggerDiscoveryMessage());

        assertTrue("Listener was never called", lsnrEntered.await(20, TimeUnit.SECONDS));

        log.info(">>>> listener entered on thread: " + lsnrThread.get());

        boolean done = lsnrDone.await(30, TimeUnit.SECONDS);

        if (done)
            log.info(">>>> RESULT: marshal COMPLETED: " + lsnrRes.get());
        else {
            log.error(">>>> RESULT: marshal DID NOT COMPLETE in 30 s -- dumping stacks");

            dumpStacks();

            fail("Binary marshalling of an unknown type hung on the discovery thread: " + lsnrThread.get());
        }
    }

    /** */
    private void dumpStacks() {
        StringBuilder sb = new StringBuilder("\n===== THREAD DUMP (discovery-related and blocked threads) =====\n");

        for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
            Thread t = e.getKey();

            String name = t.getName();

            boolean interesting = name.contains("disco") || name.contains("exchange-worker")
                || name.contains("sys-") || name.equals(lsnrThread.get());

            if (!interesting)
                continue;

            sb.append("\n\"").append(name).append("\" state=").append(t.getState()).append('\n');

            for (StackTraceElement el : e.getValue())
                sb.append("\tat ").append(el).append('\n');
        }

        sb.append("\n===== END THREAD DUMP =====\n");

        log.error(sb.toString());

        U.dumpThreads(log);
    }

    /** Control type: also unknown to the cluster, marshalled from a user thread. */
    private static class ControlType implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final int val;

        /** */
        private ControlType(int val) {
            this.val = val;
        }

        /** */
        int val() {
            return val;
        }
    }
}
