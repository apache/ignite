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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.TxMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class TxMXBeanImplTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** */
    private static final int TRANSACTIONS = 10;
    /** */
    private static final int TX_STARTUP_TIMEOUT_MS = 500;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        final CacheConfiguration cCfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setAtomicityMode(TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        super.afterTest();
    }
    /**
     *
     */
    public void testTxStop() throws Exception {
        IgniteEx ignite = startGrid(0);
        TxMXBean txMXBean = txMXBean(0);

        ignite.transactions().txStart();
        assertEquals(1, txMXBean.getAllNearTxs().size());

        txMXBean.stopTransaction(txMXBean.getAllNearTxs().keySet().iterator().next());
        assertEquals(0, txMXBean.getAllNearTxs().size());
    }

    /**
     *
     */
    public void testTxMetric() throws Exception {
        IgniteEx ignite = startGrid(0);
        startGrid(1);

        awaitPartitionMapExchange();

        TxMXBean txMXBean = txMXBean(0);

        final IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        ignite.transactions().txStart().commit();

        assertEquals(1, txMXBean.getTxCommittedNum());

        final Transaction tx1 = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        final int txNum = 10;

        for (int i = 0; i < txNum; i++)
            cache.put(i, "");

        // FIXME

//        assertEquals(txNum, txMXBean.getLockedKeysNum());
//        assertEquals(1, txMXBean.getTxHoldingLockNum());
        assertEquals(1, txMXBean.getOwnerTxNum());

        tx1.rollback();

        assertEquals(1, txMXBean.getTxRolledBackNum());

//        assertEquals(0, txMXBean.getLockedKeysNum());
//        assertEquals(0, txMXBean.getTxHoldingLockNum());
        assertEquals(0, txMXBean.getOwnerTxNum());

        block();

        for (int i = 0; i < txNum; i++)
            new Thread(new TxThread(ignite, i, i)).start();

        Thread.sleep(TX_STARTUP_TIMEOUT_MS);

//        assertEquals(txNum, txMXBean.getLockedKeysNum());
//        assertEquals(txNum, txMXBean.getTxHoldingLockNum());
        assertEquals(txNum, txMXBean.getOwnerTxNum());
    }

    /**
     *
     */
    public void testNearTxInfo() throws Exception {
        IgniteEx primaryNode1 = startGrid(0);
        IgniteEx primaryNode2 = startGrid(1);
        IgniteEx nearNode = startGrid(2);
        TxMXBean txMXBeanBackup = txMXBean(2);

        awaitPartitionMapExchange();

        final IgniteCache<Integer, String> primaryCache1 = primaryNode1.cache(DEFAULT_CACHE_NAME);
        final IgniteCache<Integer, String> primaryCache2 = primaryNode2.cache(DEFAULT_CACHE_NAME);

        final List<Integer> primaryKeys1 = primaryKeys(primaryCache1, TRANSACTIONS);
        final List<Integer> primaryKeys2 = primaryKeys(primaryCache2, TRANSACTIONS);

        block();

        for (int i = 0; i < primaryKeys1.size(); i++)
            new Thread(new TxThread(nearNode, primaryKeys1.get(i), primaryKeys2.get(i))).start();

        Thread.sleep(TX_STARTUP_TIMEOUT_MS);

        final Map<String, String> transactions = txMXBeanBackup.getAllNearTxs();
        assertEquals(TRANSACTIONS, transactions.size());

        int match = 0;
        for (String txInfo : transactions.values()) {
            if (txInfo.contains("PREPARING")
                && txInfo.contains("NEAR")
                && txInfo.contains(primaryNode1.localNode().id().toString())
                && txInfo.contains(primaryNode2.localNode().id().toString())
                && !txInfo.contains("REMOTE"))
                match++;
        }
        assertEquals(TRANSACTIONS, match);
    }

    /**
     *
     */
    private void block() {
        for (Ignite ignite : G.allGrids()) {
            final TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();
            spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtTxPrepareRequest)
                        return true;
                    else if (msg instanceof GridDhtTxPrepareResponse)
                        return true;
                    return false;
                }
            });
        }
    }

    /**
     *
     */
    private static class TxThread implements Runnable {
        /** */
        private Ignite ignite;
        /** */
        private int key1;
        /** */
        private int key2;

        /**
         * @param ignite Ignite.
         * @param key1 key 1.
         * @param key2 key 2.
         */
        private TxThread(final Ignite ignite, final int key1, final int key2) {
            this.ignite = ignite;
            this.key1 = key1;
            this.key2 = key2;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                ignite.cache(DEFAULT_CACHE_NAME).put(key1, Thread.currentThread().getName());
                ignite.cache(DEFAULT_CACHE_NAME).put(key2, Thread.currentThread().getName());
                tx.commit();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private TxMXBean txMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Transactions", TxMXBeanImpl.class.getSimpleName());
        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();
        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());
        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TxMXBean.class, true);
    }
}
