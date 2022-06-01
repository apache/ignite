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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
    private static volatile CountDownLatch txLatch;

    /** */
    private static volatile CountDownLatch cutLatch;

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

    /**
     * @param tx Function that performs transaction.
     * @param nearNodeId ID of near node.
     * @param c Test case - list of tuples (prim, backup) to be written.
     */
    protected void runCase(Runnable tx, Integer nearNodeId, List<T2<Integer, Integer>> c) throws Exception {
        cutLatch = new CountDownLatch(1);
        txLatch = new CountDownLatch(1);

        caseCnt++;

        log.info("START CASE " + caseCnt + ". Data=" + c + ", nearNodeId=" + nearNodeId);

        IgniteInternalFuture<?> txFut = multithreadedAsync(() -> {
            tx.run();

            // Some cases don't block on blkMsgCls. Then no need to await txLatch for such cases.
            txLatch.countDown();
        }, 1);

        txLatch.await(1_000, TimeUnit.MILLISECONDS);

        long cutVer = grid(0).context().cache().context().consistentCutMgr().triggerConsistentCutOnCluster();

        // Await Consistent Cut with cutVer. Set previous cutVer to `cutVer - 1`.
        awaitCutStarted(cutVer - 1);

        cutLatch.countDown();

        txFut.get(getTestTimeout());

        // Await while all async operations completed.
        Thread.sleep(10);
    }

    /**
     * Await Consistent Cut started on every node.
     *
     * @param prevCutVer Previous Consistent Cut version.
     * @return Version of the latest Consistent Cut version.
     */
    private long awaitCutStarted(long prevCutVer) throws Exception {
        long newCutVer = -1L;

        Function<Integer, ConsistentCutManager> cutMgr = (n) -> grid(n).context().cache().context().consistentCutMgr();

        int starts = 0;

        // Wait Consistent Cut locally started on every node (prepared the check-list).
        for (int n = 0; n < nodes(); n++) {
            for (int i = 0; i < 50; i++) {
                ConsistentCutState cutState = cutMgr.apply(n).latestCutState();

                long ver = cutState.version();

                if (ver > prevCutVer) {
                    if (newCutVer < 0)
                        newCutVer = ver;
                    else
                        assert newCutVer == ver : "new=" + newCutVer + ", rcv=" + ver + ", prev=" + prevCutVer;

                    if (++starts == nodes())
                        return newCutVer;

                    break;
                }

                Thread.sleep(10);
            }
        }

        StringBuilder bld = new StringBuilder()
            .append("Failed to wait Consitent Cut")
            .append(" newCutVer ").append(newCutVer)
            .append(", prevCutVer ").append(prevCutVer);

        for (int n = 0; n < nodes(); n++)
            bld.append("\nNode").append(n).append( ": ").append(cutMgr.apply(n).latestCutState());

        throw new Exception(bld.toString());
    }

    /** */
    private static class BlockingCommunicationSpi extends LogCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (blkMessage(msg)) {
                try {
                    txLatch.countDown();

                    cutLatch.await(1_000, TimeUnit.MILLISECONDS);
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

                return msg.getClass().getSimpleName().equals(blkMsgCls);
            }

            return false;
        }
    }
}
