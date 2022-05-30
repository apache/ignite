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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;

/** */
public abstract class AbstractConsistentCutBlockingTest extends AbstractConsistentCutTest {
    /** */
    protected int caseCnt;

    /** */
    protected static volatile UUID blkMsgNode;

    /** */
    private static volatile CountDownLatch latch;

    /** */
    protected static volatile String blkMsgCls;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new BlockingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).context().cache().context().consistentCutMgr().disable();
    }

    /** */
    protected void runCase(Runnable tx) throws Exception {
        runCase(tx, null, null);
    }

    /** */
    protected void runCase(Runnable tx, List<T2<Integer, Integer>> c) throws Exception {
        runCase(tx, null, c);
    }

    /** */
    protected void runCase(Runnable tx, Integer nearNodeId, List<T2<Integer, Integer>> c) throws Exception {
        latch = new CountDownLatch(1);

        caseCnt++;

        log.info("START CASE " + caseCnt + ". Data=" + c + ", nearNodeId=" + nearNodeId);

        IgniteInternalFuture<?> txFut = multithreadedAsync(tx, 1);

        long cutVer = grid(0).context().cache().context().consistentCutMgr().triggerConsistentCutOnCluster();

        // Await Consistent Cut with cutVer. Set previous cutVer to `cutVer - 1`.
        awaitConsistentCuts(1, cutVer - 1);

        latch.countDown();

        txFut.get(getTestTimeout());

        // Await while all async operations completed.
        Thread.sleep(10);
    }

    /** */
    private static class BlockingCommunicationSpi extends LogCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (blkMessage(msg)) {
                try {
                    latch.await(5_000, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /** */
        private boolean blkMessage(Message msg) {
            if (msg instanceof GridIoMessage) {
                msg = ((GridIoMessage)msg).message();

                UUID nodeId = getSpiContext().localNode().id();

                return nodeId.equals(blkMsgNode) && msg.getClass().getSimpleName().equals(blkMsgCls);
            }

            return false;
        }
    }
}
