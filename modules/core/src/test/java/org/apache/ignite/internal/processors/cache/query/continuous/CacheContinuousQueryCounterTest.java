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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheContinuousQueryCounterTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private static final int KEYS = 50;

    /** */
    private static final int VALS = 10;

    /** */
    public static final int ITERATION_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);
        cfg.setLateAffinityAssignment(true);

        return cfg;
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("test-cache");

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailedNode() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            100,
            ATOMIC,
            ONHEAP_TIERED);

        ccfg.setAffinity(new TestAffinityFunction());

        final ExecutorService exec = Executors.newSingleThreadExecutor();

        for (int i = 0; i < 10; i++) {
            Ignite ignite = startGrid(0);

            final IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

            ContinuousQuery qry = new ContinuousQuery();
            final AtomicInteger countOfEvets = new AtomicInteger();

            qry.setLocalListener(new CacheEntryUpdatedListener() {
                @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                    for (Object o : iterable) {
                        countOfEvets.incrementAndGet();

                        System.out.println("Received event: " + o);
                    }
                }
            });

            QueryCursor cur = cache.query(qry);

            final AtomicInteger cntr = new AtomicInteger();

            ignite.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event event) {
                    exec.submit(new Runnable() {
                        @Override public void run() {
                            cache.put(0, cntr.incrementAndGet());
                        }
                    });

                    return true;
                }
            }, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_JOINED);

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            boolean b = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return countOfEvets.get() == 2;
                }
            }, 10_000);

            assertTrue(b);

            exec.submit(new Runnable() {
                @Override public void run() {
                    stopGrid(2);
                }
            });
            exec.submit(new Runnable() {
                @Override public void run() {
                    stopGrid(1);
                }
            });

            b = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return countOfEvets.get() == 4;
                }
            }, 10_000);

            assertTrue(b);

            cache.put(0, 42);
            cache.put(0, 43);

            b = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return countOfEvets.get() == 6;
                }
            }, 10_000);

            assertTrue(b);

            cur.close();

            stopAllGrids();
        }
    }

    /**
     *
     */
    public static class TestAffinityFunction implements AffinityFunction {
        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> ass = new ArrayList<>(1);

            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            if (nodes.size() != 2)
                Collections.sort(nodes, new Comparator<ClusterNode>() {
                    @Override public int compare(ClusterNode o1, ClusterNode o2) {
                        return (int)(o1.order() - o2.order());
                    }
                });
            else
                Collections.sort(nodes, new Comparator<ClusterNode>() {
                    @Override public int compare(ClusterNode o1, ClusterNode o2) {
                        return (int)(o2.order() - o1.order());
                    }
                });

            ass.add(0, nodes);

            System.out.println("Assignment: " + Arrays.toString(ass.toArray()));

            return ass;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }
}
