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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.io.*;
import java.util.concurrent.*;

import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 */
public class GridCacheMarshallingNodeJoinSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, TestObject> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setCacheStoreFactory(new StoreFactory());
        cacheCfg.setReadThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoin() throws Exception {
        final CountDownLatch allowJoin = new CountDownLatch(1);
        final CountDownLatch joined = new CountDownLatch(2);

        for (int i = 0; i < 2; i++) {
            ignite(i).events().localListen(new PE() {
                @Override public boolean apply(Event evt) {
                    assert evt.type() == EventType.EVT_NODE_JOINED;

                    info(">>> Event: " + evt);

                    joined.countDown();

                    return true;
                }
            }, EventType.EVT_NODE_JOINED);
        }

        IgniteInternalFuture<?> oneMoreGrid = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                allowJoin.await();

                startGrid("oneMoreGrid");

                return null;
            }
        }, 1);

        IgniteCache<Integer, TestObject> cache = ignite(0).cache(null);

        try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.get(0);

            allowJoin.countDown();

            joined.await();

            assertNotNull(cache.get(1));

            tx.commit();
        }

        oneMoreGrid.get();

        assertNotNull(cache.get(1));
    }

    /**
     */
    private static class StoreFactory implements Factory<CacheStore<? super Integer, ? super TestObject>> {
        /** {@inheritDoc} */
        @Override public CacheStore<? super Integer, ? super TestObject> create() {
            return new Store();
        }
    }

    /**
     */
    private static class Store extends CacheStoreAdapter<Integer, TestObject> implements Serializable {
        /** {@inheritDoc} */
        @Override public TestObject load(Integer key) throws CacheLoaderException {
            return key > 0 ? new TestObject() : null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends TestObject> e)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }

    /**
     */
    private static class TestObject implements Serializable {
        /**
         */
        public TestObject() {
            // No-op.
        }
    }
}
