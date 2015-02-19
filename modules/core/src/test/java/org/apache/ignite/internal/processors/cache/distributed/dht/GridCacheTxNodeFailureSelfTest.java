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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests one-phase commit transactions when some of the nodes fail in the middle of the transaction.
 */
public class GridCacheTxNodeFailureSelfTest extends GridCommonAbstractTest {
    /**
     * @return Grid count.
     */
    public int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new BanningCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupCommit() throws Exception {
        startGrids(gridCount());
        awaitPartitionMapExchange();

        try {
            final Ignite ignite = ignite(0);

            final IgniteCache<Object, Object> cache = ignite.jcache(null);

            final int key = generateKey(ignite);

            final CountDownLatch commitLatch = new CountDownLatch(1);

            communication(2).bannedClasses(Collections.<Class>singletonList(GridDhtTxPrepareResponse.class));
            communication(3).bannedClasses(Collections.<Class>singletonList(GridDhtTxPrepareResponse.class));

            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Transaction tx = ignite.transactions().txStart()) {
                        cache.put(key, key);

                        Transaction asyncTx = (Transaction)tx.withAsync();

                        asyncTx.commit();

                        commitLatch.countDown();

                        asyncTx.future().get();
                    }

                    return null;
                }
            });

            commitLatch.await();

            stopGrid(1);

            // No exception should happen since transaction is committed on the backup node.
            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param idx Index.
     * @return Communication SPI.
     */
    private BanningCommunicationSpi communication(int idx) {
        return (BanningCommunicationSpi)ignite(idx).configuration().getCommunicationSpi();
    }

    /**
     * @param ignite Ignite instance to generate key.
     * @return Generated key that is not primary nor backup for {@code ignite(0)} and primary for
     *      {@code ignite(1)}.
     */
    private int generateKey(Ignite ignite) {
        CacheAffinity<Object> aff = ignite.affinity(null);

        for (int key = 0;;key++) {
            if (aff.isPrimaryOrBackup(ignite(0).cluster().localNode(), key))
                continue;

            if (aff.isPrimary(ignite(1).cluster().localNode(), key))
                return key;
        }
    }

    /**
     *
     */
    private static class BanningCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile Collection<Class> bannedClasses = Collections.emptyList();

        /**
         * @param bannedClasses Banned classes.
         */
        public void bannedClasses(Collection<Class> bannedClasses) {
            this.bannedClasses = bannedClasses;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, MessageAdapter msg) throws IgniteSpiException {
            if (!bannedClasses.contains(msg.getClass()))
                super.sendMessage(node, msg);
        }
    }
}
