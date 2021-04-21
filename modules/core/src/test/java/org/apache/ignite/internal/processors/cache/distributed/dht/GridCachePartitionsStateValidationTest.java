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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionsStateValidator;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 *
 */
public class GridCachePartitionsStateValidationTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        cfg.setCommunicationSpi(new SingleMessageInterceptorCommunicationSpi(2));

        return cfg;
    }

    /** {@inheritDoc */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test that partitions state validation works correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationIfPartitionCountersAreInconsistent() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrids(2);
        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        // Populate cache to increment update counters.
        for (int i = 0; i < 1000; i++)
            ignite.cache(CACHE_NAME).put(i, i);

        // Modify update counter for some partition.
        for (GridDhtLocalPartition partition : ignite.cachex(CACHE_NAME).context().topology().localPartitions()) {
            partition.updateCounter(100500L);
            break;
        }

        // Trigger exchange.
        startGrid(2);

        awaitPartitionMapExchange();

        // Nothing should happen (just log error message) and we're still able to put data to corrupted cache.
        ignite.cache(CACHE_NAME).put(0, 0);

        stopAllGrids();
    }

    /**
     * Test that all nodes send correct {@link GridDhtPartitionsSingleMessage} with consistent update counters.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionCountersConsistencyOnExchange() throws Exception {
        // Reopen https://issues.apache.org/jira/browse/IGNITE-10766 if starts failing with forced MVCC

        IgniteEx ignite = startGrids(4);
        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        final String atomicCacheName = "atomic-cache";
        final String txCacheName = "tx-cache";

        Ignite client = startClientGrid(4);

        IgniteCache atomicCache = client.getOrCreateCache(new CacheConfiguration<>(atomicCacheName)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        IgniteCache txCache = client.getOrCreateCache(new CacheConfiguration<>(txCacheName)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        for (int it = 0; it < 10; it++) {
            SingleMessageInterceptorCommunicationSpi spi = (SingleMessageInterceptorCommunicationSpi) ignite.configuration().getCommunicationSpi();
            spi.clear();

            // Stop load future.
            final AtomicBoolean stop = new AtomicBoolean();

            // Run atomic load.
            IgniteInternalFuture atomicLoadFuture = GridTestUtils.runMultiThreadedAsync(() -> {
                int k = 0;

                while (!stop.get()) {
                    k++;
                    try {
                        atomicCache.put(k, k);
                    } catch (Exception ignored) {}
                }
            }, 1, "atomic-load");

            // Run tx load.
            IgniteInternalFuture txLoadFuture = GridTestUtils.runMultiThreadedAsync(() -> {
                final int txOps = 5;

                while (!stop.get()) {
                    List<Integer> randomKeys = Stream.generate(() -> ThreadLocalRandom.current().nextInt(5))
                        .limit(txOps)
                        .sorted()
                        .collect(Collectors.toList());

                    try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                        for (Integer key : randomKeys)
                            txCache.put(key, key);

                        tx.commit();
                    }
                    catch (Exception ignored) { }
                }
            }, 4, "tx-load");

            // Wait for some data.
            Thread.sleep(1000);

            // Prevent sending full message.
            spi.blockFullMessage();

            // Trigger exchange.
            IgniteInternalFuture nodeStopFuture = GridTestUtils.runAsync(() -> stopGrid(3));

            try {
                spi.waitUntilAllSingleMessagesAreSent();

                List<GridDhtPartitionsSingleMessage> interceptedMessages = spi.getMessages();

                // Associate each message with existing node UUID.
                Map<UUID, GridDhtPartitionsSingleMessage> messagesMap = new HashMap<>();
                for (int i = 0; i < interceptedMessages.size(); i++)
                    messagesMap.put(grid(i + 1).context().localNodeId(), interceptedMessages.get(i));

                GridDhtPartitionsStateValidator validator = new GridDhtPartitionsStateValidator(ignite.context().cache().context());

                // Validate partition update counters. If counters are not consistent, exception will be thrown.
                validator.validatePartitionsUpdateCounters(ignite.cachex(atomicCacheName).context().topology(), messagesMap, Collections.emptySet());
                validator.validatePartitionsUpdateCounters(ignite.cachex(txCacheName).context().topology(), messagesMap, Collections.emptySet());

            } finally {
                // Stop load and resume exchange.
                spi.unblockFullMessage();

                stop.set(true);

                atomicLoadFuture.get();
                txLoadFuture.get();
                nodeStopFuture.get();
            }

            // Return grid to initial state.
            startGrid(3);

            awaitPartitionMapExchange();
        }
    }

    /**
     * SPI which intercepts single messages during exchange.
     */
    private static class SingleMessageInterceptorCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private static final List<GridDhtPartitionsSingleMessage> messages = new CopyOnWriteArrayList<>();

        /** Future completes when {@link #singleMessagesThreshold} messages are sent to coordinator. */
        private static final GridFutureAdapter allSingleMessagesSent = new GridFutureAdapter();

        /** A number of single messages we're waiting for send. */
        private final int singleMessagesThreshold;

        /** Latch which blocks full message sending. */
        private volatile CountDownLatch blockFullMsgLatch;

        /**
         * Constructor.
         */
        private SingleMessageInterceptorCommunicationSpi(int singleMessagesThreshold) {
            this.singleMessagesThreshold = singleMessagesThreshold;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (((GridIoMessage) msg).message() instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage) ((GridIoMessage) msg).message();

                // We're interesting for only exchange messages and when node is stopped.
                if (singleMsg.exchangeId() != null && singleMsg.exchangeId().isLeft() && !singleMsg.client()) {
                    messages.add(singleMsg);

                    if (messages.size() == singleMessagesThreshold)
                        allSingleMessagesSent.onDone();
                }
            }

            try {
                if (((GridIoMessage) msg).message() instanceof GridDhtPartitionsFullMessage) {
                    if (blockFullMsgLatch != null)
                        blockFullMsgLatch.await();
                }
            }
            catch (Exception ignored) { }

            super.sendMessage(node, msg, ackC);
        }

        /** */
        public void clear() {
            messages.clear();
            allSingleMessagesSent.reset();
        }

        /** */
        public List<GridDhtPartitionsSingleMessage> getMessages() {
            return Collections.unmodifiableList(messages);
        }

        /** */
        public void blockFullMessage() {
            blockFullMsgLatch = new CountDownLatch(1);
        }

        /** */
        public void unblockFullMessage() {
            blockFullMsgLatch.countDown();
        }

        /** */
        public void waitUntilAllSingleMessagesAreSent() throws IgniteCheckedException {
            allSingleMessagesSent.get();
        }
    }
}
