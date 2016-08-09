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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.*;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.REMOVED;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest.ContinuousDeploy.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class CacheEntryProcessorNonSerialazibleTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 2;

    /** */
    public static final int ITERATION_CNT = 100;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);
        cfg.setMarshaller(new OptimizedMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(getServerNodeCount());

        client = true;

        startGrid(getServerNodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @return Server nodes.
     */
    private int getServerNodeCount() {
        return NODES;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PRIMARY_SYNC, 2);

        doTestInvokeTest(ccfg);
    }

    private void doTestInvokeTest(CacheConfiguration ccfg) throws Exception {
        IgniteEx cln = grid(getServerNodeCount());
        IgniteCache clnCache = cln.createCache(ccfg);

        try {
            for (int i = 0; i < ITERATION_CNT; i++) {
                try (Transaction tx = cln.transactions().txStart(TransactionConcurrency.PESSIMISTIC, REPEATABLE_READ)) {
                    try {
                        clnCache.invoke(1, new NonSerialazibleEntryProcessor());

                        tx.commit();
                    } catch (Exception e) {
                        e.printStackTrace();

                        tx.rollback();
                    }
                }
            }

        }
        finally {
            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheWriteSynchronizationMode wrMode, int backup) {
        return new CacheConfiguration("test-cache-" + wrMode + "-" + backup)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC);
            //.setBackups(backup);
    }

    /**
     *
     */
    private static class NonSerialazibleEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
            throws EntryProcessorException {
            entry.setValue(42);

            return null;
        }
    }

    public static void main(String[] args) {
        try (
                Ignite server = Ignition.start(config(false));
                Ignite client = Ignition.start(config(true))
        ) {
            CacheConfiguration<Integer, Integer> cacheConfig = new CacheConfiguration<>("test-cache");

            cacheConfig.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            IgniteCache<Integer, Integer> cache = client.createCache(cacheConfig);

            for (int i = 0; i < 10; i++) {
                try (Transaction tx = client.transactions().txStart()) {
                    try {
                        cache.invoke(1, new EntryProcessor<Integer, Integer, Boolean>() {
                            @Override public Boolean process(MutableEntry<Integer, Integer> entry, Object... args) {
                                return Boolean.TRUE;
                            }
                        });

                        tx.commit();

                        System.out.println("Commit #" + i);
                    }
                    catch (RuntimeException e) {
                        tx.rollback();

                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static IgniteConfiguration config(boolean client) {
        IgniteConfiguration config = new IgniteConfiguration();

        config.setClientMode(client);
        config.setGridName(client ? "client" : "server");
        config.setMarshaller(new OptimizedMarshaller());

        return config;
    }
}
