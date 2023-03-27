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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 * Test implementation for blocking rebalance process.
 */
public class RebalanceBlockingSPI extends TcpCommunicationSpi {
    /** Supply message latch. */
    private final AtomicReference<CountDownLatch> supplyMsgLatch;

    /** Slow rebalance cache name. */
    private final String cacheName;

    /** Supply message latch. */
    private final AtomicReference<CountDownLatch> supplyMsgSndLatch;

    /**
     * @param supplyMsgLatch Supply message latch.
     * @param cacheName Cache name.
     * @param supplyMsgSndLatch Supply message sender latch.
     */
    public RebalanceBlockingSPI(
        AtomicReference<CountDownLatch> supplyMsgLatch,
        String cacheName,
        AtomicReference<CountDownLatch> supplyMsgSndLatch
    ) {
        this.supplyMsgLatch = supplyMsgLatch;
        this.cacheName = cacheName;
        this.supplyMsgSndLatch = supplyMsgSndLatch;
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
        processMessage(msg);

        super.sendMessage(node, msg);
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(
        ClusterNode node,
        Message msg,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteSpiException {
        processMessage(msg);

        super.sendMessage(node, msg, ackC);
    }

    /**
     * @param msg Message.
     */
    private void processMessage(Message msg) {
        if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
            int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

            if (grpId == CU.cacheId(cacheName)) {
                CountDownLatch latch0 = supplyMsgLatch.get();

                Optional.ofNullable(supplyMsgSndLatch.get()).ifPresent(CountDownLatch::countDown);

                if (latch0 != null)
                    try {
                        latch0.await();
                    }
                    catch (InterruptedException ex) {
                        throw new IgniteException(ex);
                    }
            }
        }
    }
}
