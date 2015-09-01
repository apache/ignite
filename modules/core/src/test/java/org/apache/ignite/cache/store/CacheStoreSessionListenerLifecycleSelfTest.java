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

package org.apache.ignite.cache.store;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Store session listeners test.
 */
public class CacheStoreSessionListenerLifecycleSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final Queue<String> evts = new ConcurrentLinkedDeque<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheStoreSessionListenerFactories(
            new SessionListenerFactory("Shared 1"),
            new SessionListenerFactory("Shared 2")
        );

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        evts.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoCaches() throws Exception {
        try {
            startGrid();
        }
        finally {
            stopGrid();
        }

        assertEqualsCollections(Arrays.asList("Shared 1 START", "Shared 2 START", "Shared 1 STOP", "Shared 2 STOP"),
            evts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoOverride() throws Exception {
        try {
            Ignite ignite = startGrid();

            for (int i = 0; i < 2; i++) {
                CacheConfiguration<Integer, Integer> cacheCfg = cacheConfiguration("cache-" + i);

                cacheCfg.setAtomicityMode(TRANSACTIONAL);

                ignite.createCache(cacheCfg);
            }

            ignite.cache("cache-0").put(1, 1);
            ignite.cache("cache-1").put(1, 1);

            try (Transaction tx = ignite.transactions().txStart()) {
                ignite.cache("cache-0").put(2, 2);
                ignite.cache("cache-0").put(3, 3);
                ignite.cache("cache-1").put(2, 2);
                ignite.cache("cache-1").put(3, 3);

                tx.commit();
            }
        }
        finally {
            stopGrid();
        }

        assertEqualsCollections(Arrays.asList(
            "Shared 1 START",
            "Shared 2 START",

            // Put to cache-0.
            "Shared 1 SESSION START cache-0",
            "Shared 2 SESSION START cache-0",
            "Shared 1 SESSION END cache-0",
            "Shared 2 SESSION END cache-0",

            // Put to cache-1.
            "Shared 1 SESSION START cache-1",
            "Shared 2 SESSION START cache-1",
            "Shared 1 SESSION END cache-1",
            "Shared 2 SESSION END cache-1",

            // Transaction.
            "Shared 1 SESSION START cache-0",
            "Shared 2 SESSION START cache-0",
            "Shared 1 SESSION START cache-1",
            "Shared 2 SESSION START cache-1",
            "Shared 1 SESSION END cache-0",
            "Shared 2 SESSION END cache-0",
            "Shared 1 SESSION END cache-1",
            "Shared 2 SESSION END cache-1",

            "Shared 1 STOP",
            "Shared 2 STOP"
        ), evts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartialOverride() throws Exception {
        try {
            Ignite ignite = startGrid();

            for (int i = 0; i < 2; i++) {
                String name = "cache-" + i;

                CacheConfiguration cacheCfg = cacheConfiguration(name);

                cacheCfg.setAtomicityMode(TRANSACTIONAL);

                if (i == 0) {
                    cacheCfg.setCacheStoreSessionListenerFactories(
                        new SessionListenerFactory(name + " 1"),
                        new SessionListenerFactory(name + " 2")
                    );
                }

                ignite.createCache(cacheCfg);
            }

            ignite.cache("cache-0").put(1, 1);
            ignite.cache("cache-1").put(1, 1);

            try (Transaction tx = ignite.transactions().txStart()) {
                ignite.cache("cache-0").put(2, 2);
                ignite.cache("cache-0").put(3, 3);
                ignite.cache("cache-1").put(2, 2);
                ignite.cache("cache-1").put(3, 3);

                tx.commit();
            }
        }
        finally {
            stopGrid();
        }

        assertEqualsCollections(Arrays.asList(
            "Shared 1 START",
            "Shared 2 START",
            "cache-0 1 START",
            "cache-0 2 START",

            // Put to cache-0.
            "cache-0 1 SESSION START cache-0",
            "cache-0 2 SESSION START cache-0",
            "cache-0 1 SESSION END cache-0",
            "cache-0 2 SESSION END cache-0",

            // Put to cache-1.
            "Shared 1 SESSION START cache-1",
            "Shared 2 SESSION START cache-1",
            "Shared 1 SESSION END cache-1",
            "Shared 2 SESSION END cache-1",

            // Transaction.
            "cache-0 1 SESSION START cache-0",
            "cache-0 2 SESSION START cache-0",
            "Shared 1 SESSION START cache-1",
            "Shared 2 SESSION START cache-1",
            "cache-0 1 SESSION END cache-0",
            "cache-0 2 SESSION END cache-0",
            "Shared 1 SESSION END cache-1",
            "Shared 2 SESSION END cache-1",

            "cache-0 1 STOP",
            "cache-0 2 STOP",
            "Shared 1 STOP",
            "Shared 2 STOP"
        ), evts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverride() throws Exception {
        try {
            Ignite ignite = startGrid();

            for (int i = 0; i < 2; i++) {
                String name = "cache-" + i;

                CacheConfiguration cacheCfg = cacheConfiguration(name);

                cacheCfg.setCacheStoreSessionListenerFactories(new SessionListenerFactory(name + " 1"), new SessionListenerFactory(name + " 2"));

                ignite.createCache(cacheCfg);
            }

            ignite.cache("cache-0").put(1, 1);
            ignite.cache("cache-1").put(1, 1);

            try (Transaction tx = ignite.transactions().txStart()) {
                ignite.cache("cache-0").put(2, 2);
                ignite.cache("cache-0").put(3, 3);
                ignite.cache("cache-1").put(2, 2);
                ignite.cache("cache-1").put(3, 3);

                tx.commit();
            }
        }
        finally {
            stopGrid();
        }

        assertEqualsCollections(Arrays.asList(
            "Shared 1 START",
            "Shared 2 START",
            "cache-0 1 START",
            "cache-0 2 START",
            "cache-1 1 START",
            "cache-1 2 START",

            // Put to cache-0.
            "cache-0 1 SESSION START cache-0",
            "cache-0 2 SESSION START cache-0",
            "cache-0 1 SESSION END cache-0",
            "cache-0 2 SESSION END cache-0",

            // Put to cache-1.
            "cache-1 1 SESSION START cache-1",
            "cache-1 2 SESSION START cache-1",
            "cache-1 1 SESSION END cache-1",
            "cache-1 2 SESSION END cache-1",

            // Transaction.
            "cache-0 1 SESSION START cache-0",
            "cache-0 2 SESSION START cache-0",
            "cache-1 1 SESSION START cache-1",
            "cache-1 2 SESSION START cache-1",
            "cache-0 1 SESSION END cache-0",
            "cache-0 2 SESSION END cache-0",
            "cache-1 1 SESSION END cache-1",
            "cache-1 2 SESSION END cache-1",

            "cache-0 1 STOP",
            "cache-0 2 STOP",
            "cache-1 1 STOP",
            "cache-1 2 STOP",
            "Shared 1 STOP",
            "Shared 2 STOP"
        ), evts);
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name) {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(name);

        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(Store.class));
        cacheCfg.setWriteThrough(true);

        return cacheCfg;
    }

    /**
     */
    private static class SessionListener implements CacheStoreSessionListener, LifecycleAware {
        /** */
        private final String name;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param name Name.
         */
        private SessionListener(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {
            assertNotNull(ignite);

            evts.add(name + " START");
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteException {
            assertNotNull(ignite);

            evts.add(name + " STOP");
        }

        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {
            assertNotNull(ignite);

            evts.add(name + " SESSION START " + ses.cacheName());
        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
            assertNotNull(ignite);

            evts.add(name + " SESSION END " + ses.cacheName());
        }
    }

    /**
     */
    private static class SessionListenerFactory implements Factory<CacheStoreSessionListener> {
        /** */
        private String name;

        /**
         * @param name Name.
         */
        private SessionListenerFactory(String name) {
            this.name = name;
        }

        @Override public CacheStoreSessionListener create() {
            return new SessionListener(name);
        }
    }

    /**
     */
    public static class Store extends CacheStoreAdapter<Integer, Integer> {
        public Store() {
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}