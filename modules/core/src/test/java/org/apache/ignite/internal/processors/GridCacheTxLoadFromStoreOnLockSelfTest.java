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

package org.apache.ignite.internal.processors;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.io.*;

/**
 *
 */
public class GridCacheTxLoadFromStoreOnLockSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadedValueOneBackup() throws Exception {
        checkLoadedValue(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadedValueNoBackups() throws Exception {
        checkLoadedValue(0);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkLoadedValue(int backups) throws Exception {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setCacheStoreFactory(new StoreFactory());
        cacheCfg.setReadThrough(true);
        cacheCfg.setBackups(backups);
        cacheCfg.setLoadPreviousValue(true);

        try (IgniteCache<Integer, Integer> cache = ignite(0).createCache(cacheCfg)) {
            for (int i = 0; i < 10; i++)
                assertEquals((Integer)i, cache.get(i));

            cache.removeAll();

            assertEquals(0, cache.size());

            for (TransactionConcurrency conc : TransactionConcurrency.values()) {
                for (TransactionIsolation iso : TransactionIsolation.values()) {
                    info("Checking transaction [conc=" + conc + ", iso=" + iso + ']');

                    try (Transaction tx = ignite(0).transactions().txStart(conc, iso)) {
                        for (int i = 0; i < 10; i++)
                            assertEquals("Invalid value for transaction [conc=" + conc + ", iso=" + iso + ']',
                                (Integer)i, cache.get(i));

                        tx.commit();
                    }

                    cache.removeAll();
                    assertEquals(0, cache.size());
                }
            }
        }
    }

    /**
     *
     */
    private static class StoreFactory implements Factory<CacheStore<? super Integer, ? super Integer>> {
        /** {@inheritDoc} */
        @Override public CacheStore<? super Integer, ? super Integer> create() {
            return new Store();
        }
    }

    /**
     *
     */
    private static class Store extends CacheStoreAdapter<Integer, Integer> implements Serializable {
        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> e)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}
