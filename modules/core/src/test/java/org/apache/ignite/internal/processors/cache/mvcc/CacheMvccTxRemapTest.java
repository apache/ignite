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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistRequest;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public class CacheMvccTxRemapTest extends CacheMvccAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    public static final int PARTITIONS = 3;

    /** */
    private Phaser phaser;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestDelayingCommunicationSpi() {
                @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
                    if (msg instanceof GridNearTxEnlistRequest &&
                        ((GridNearTxEnlistRequest)msg).firstClientRequest()) {

                        phaser.arriveAndAwaitAdvance();
                    }

                    return false;
                }

                @Override protected int delayMillis() {
                    return 1000;
                }
            });
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * Creates cache configuration.
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<?, ?> cacheConfiguration(String cacheName) {
        return new CacheConfiguration<>(cacheName)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            .setAffinity(new MyAffinityFunction(PARTITIONS))
            .setBackups(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemap() throws Exception {
        disableScheduledVacuum = true;

        phaser = new Phaser(2);

        startGrid(0);
        startGrid(1);

        client = true;
        Ignite cli = startGrid(2);
        client = false;

        IgniteCache cache = cli.getOrCreateCache(cacheConfiguration(CACHE_NAME));

        awaitPartitionMapExchange();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    while (!stop.get()) {
                        if (phaser.arriveAndAwaitAdvance() < 0)
                            return; // Terminated.

                        startGrid(3);

                        if (stop.get() || phaser.arriveAndAwaitAdvance() < 0)
                            return;

                        stopGrid(3);
                    }
                }
                catch (Exception e) {
                    throw new AssertionError(e);
                }
                finally {
                    stop.set(true);
                }
            }
        });

        IgniteInternalFuture fut1 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                int iter = 0;

                final HashMap<Object, Object> map = new HashMap<>();

                while (!stop.get()) {
                    for (int i = iter; i < 100; i += 3)
                        map.put(i, iter);

                    IgniteTransactions txs = cli.transactions();
                    try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                        cache.putAll(map);

                        tx.commit();
                    }
                    catch (CacheException e) {
                        ClusterTopologyException cause = X.cause(e, ClusterTopologyException.class);

                        if (cause != null &&
                            cause.getMessage().startsWith("Failed to get primary") ||
                            cause.getMessage().startsWith("Backup node left the grid"))
                            continue;

                        throw e;
                    }

                    iter++;
                }
            }
        });

        boolean cond;
        try {
            cond = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return fut.isDone() || fut1.isDone();
                }
            }, getTestTimeout()/2);
        }
        finally {
            stop.set(true);

            phaser.forceTermination(); // Release threads.
        }

        fut.get();
        fut1.get();

        assertFalse(cond);
    }

    /**
     *
     */
    private static class MyAffinityFunction implements AffinityFunction {
        /** */
        private int parts;

        /** */
        private MyAffinityFunction(int parts) {
            this.parts = parts;
        }

        /** {@inheritDoc} */
        @Override public void reset() {

        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return parts;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return key.hashCode() % parts;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            ClusterNode[] nodes = affCtx.currentTopologySnapshot().toArray(new ClusterNode[0]);

            int backups = Math.min(affCtx.backups(), nodes.length - 1);

            List<List<ClusterNode>> assignment = new ArrayList<>(parts);

            for (int p = 0; p < parts; p++) {
                List<ClusterNode> mapping = new ArrayList<>(backups);

                int primeIdx = p % nodes.length;

                for (int i = 0; i < 1 + backups; i++)
                    mapping.add(nodes[(primeIdx + i) % nodes.length]);

                assignment.add(mapping);
            }

            return assignment;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {

        }
    }
}
