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

public class RebalanceBlockingSPI extends TcpCommunicationSpi {
    /** Supply message latch. */
    private final AtomicReference<CountDownLatch> supplyMsgLatch;

    /** Slow rebalance cache name. */
    private final String cacheName;

    /** Supply message latch. */
    private final AtomicReference<CountDownLatch> supplyMsgSndLatch;

    public RebalanceBlockingSPI(AtomicReference<CountDownLatch> latch, String name,
        AtomicReference<CountDownLatch> supplyMsgSndLatch) {
        supplyMsgLatch = latch;
        cacheName = name;
        this.supplyMsgSndLatch = supplyMsgSndLatch;
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
        if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
            int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

            if (grpId == CU.cacheId(cacheName)) {
                CountDownLatch latch0 = supplyMsgLatch.get();

                if (latch0 != null)
                    try {
                        latch0.await();
                    }
                    catch (InterruptedException ex) {
                        throw new IgniteException(ex);
                    }
            }
        }

        super.sendMessage(node, msg);
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg,
        IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
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

        super.sendMessage(node, msg, ackC);
    }
}
