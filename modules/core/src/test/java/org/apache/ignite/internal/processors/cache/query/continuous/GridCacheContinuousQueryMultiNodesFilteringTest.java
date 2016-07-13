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

import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;

/** */
@SuppressWarnings("unchecked")
public class GridCacheContinuousQueryMultiNodesFilteringTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SERVER_GRIDS_COUNT = 6;

    /** Cache entry operations' counts. */
    private static final ConcurrentMap<String, AtomicInteger> opCounts = new ConcurrentHashMap8<>();

    /** */
    public void testFiltersAndListeners() throws Exception {
        for (int i = 1; i <= SERVER_GRIDS_COUNT; i++)
            startGrid(i, false);
        startGrid(SERVER_GRIDS_COUNT + 1, true);

        for (int i = 1; i <= SERVER_GRIDS_COUNT + 1; i++)
            for (int j = 0; j < i; j++) {
                jcache(i, "part" + i).put("k" + j, "v0");
                jcache(i, "repl" + i).put("k" + j, "v0");

                // Should trigger updates
                jcache(i, "part" + i).put("k" + j, "v1");
                jcache(i, "repl" + i).put("k" + j, "v1");

                jcache(i, "part" + i).remove("k" + j);
                jcache(i, "repl" + i).remove("k" + j);
            }

        for (int i = 1; i <= SERVER_GRIDS_COUNT + 1; i++) {
            int partInserts = opCounts.get("part" + i + "_ins").get();
            int replInserts = opCounts.get("repl" + i + "_ins").get();
            int partUpdates = opCounts.get("part" + i + "_upd").get();
            int replUpdates = opCounts.get("repl" + i + "_upd").get();
            int partRemoves = opCounts.get("part" + i + "_rmv").get();
            int replRemoves = opCounts.get("repl" + i + "_rmv").get();
            int totalQryOps = opCounts.get("qry"  + i + "_total").get();

            assertEquals(i, partInserts);
            assertEquals(i, replInserts);

            assertEquals(i, partUpdates);
            assertEquals(i, replUpdates);

            assertEquals(i, partRemoves);
            assertEquals(i, replRemoves);

            // For each i, we did 3 ops on 2 caches on i keys, hence expected number
            assertEquals(i * 3 * 2, totalQryOps);

            assertEquals(totalQryOps, partInserts + replInserts + partUpdates + replUpdates + partRemoves + replRemoves);
        }
    }

    /** */
    private Ignite startGrid(final int idx, boolean isClientMode) throws Exception {
        String gridName = getTestGridName(idx);
        IgniteConfiguration cfg = optimize(getConfiguration(gridName)).setClientMode(isClientMode);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);
        cfg.setUserAttributes(Collections.singletonMap("idx", idx));
        Ignite node = startGrid(gridName, cfg);
        IgnitePredicate<ClusterNode> nodeFilter = new NodeFilter(idx);

        String partCacheName = "part" + idx;

        IgniteCache partCache = node.createCache(defaultCacheConfiguration().setName("part" + idx)
            .setCacheMode(CacheMode.PARTITIONED).setNodeFilter(nodeFilter));

        opCounts.put(partCacheName + "_ins", new AtomicInteger());
        opCounts.put(partCacheName + "_upd", new AtomicInteger());
        opCounts.put(partCacheName + "_rmv", new AtomicInteger());

        partCache.registerCacheEntryListener(new EntryLsnrCfg(partCacheName, EntryLsnrCfg.Op.INS));
        partCache.registerCacheEntryListener(new EntryLsnrCfg(partCacheName, EntryLsnrCfg.Op.UPD));
        partCache.registerCacheEntryListener(new EntryLsnrCfg(partCacheName, EntryLsnrCfg.Op.RMV));

        String replCacheName = "repl" + idx;

        IgniteCache replCache = node.createCache(defaultCacheConfiguration().setName("repl" + idx)
            .setCacheMode(CacheMode.REPLICATED).setBackups(1).setNodeFilter(nodeFilter));

        opCounts.put(replCacheName + "_ins", new AtomicInteger());
        opCounts.put(replCacheName + "_upd", new AtomicInteger());
        opCounts.put(replCacheName + "_rmv", new AtomicInteger());

        replCache.registerCacheEntryListener(new EntryLsnrCfg(replCacheName, EntryLsnrCfg.Op.INS));
        replCache.registerCacheEntryListener(new EntryLsnrCfg(replCacheName, EntryLsnrCfg.Op.UPD));
        replCache.registerCacheEntryListener(new EntryLsnrCfg(replCacheName, EntryLsnrCfg.Op.RMV));

        opCounts.put("qry" + idx + "_total", new AtomicInteger());

        ContinuousQuery qry = new ContinuousQuery();
        qry.setRemoteFilterFactory(new EntryEventFilterFactory(idx));
        qry.setLocalListener(new CacheEntryUpdatedListener() {
            /** {@inheritDoc} */
            @Override public void onUpdated(Iterable evts) {
                opCounts.get("qry" + idx + "_total").incrementAndGet();
            }
        });

        partCache.query(qry);
        replCache.query(qry);

        return node;
    }

    /** */
    private final static class EntryLsnrCfg extends MutableCacheEntryListenerConfiguration {
        /** Operation. */
        enum Op {
            /** Insert. */
            INS,

            /** Update. */
            UPD,

            /** Remove. */
            RMV
        }

        /** */
        EntryLsnrCfg(final String cacheName, final Op op) {
            super(new Factory<CacheEntryListener>() {
                /** {@inheritDoc} */
                @Override public CacheEntryListener create() {
                    switch (op) {
                        case INS:
                            return new CacheEntryCreatedListener() {
                                /** {@inheritDoc} */
                                @Override public void onCreated(Iterable iterable) throws CacheEntryListenerException {
                                    opCounts.get(cacheName + "_ins").getAndIncrement();
                                }
                            };
                        case UPD:
                            return new CacheEntryUpdatedListener() {
                                /** {@inheritDoc} */
                                @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                                    opCounts.get(cacheName + "_upd").getAndIncrement();
                                }
                            };
                        case RMV:
                            return new CacheEntryRemovedListener() {
                                /** {@inheritDoc} */
                                @Override public void onRemoved(Iterable iterable) throws CacheEntryListenerException {
                                    opCounts.get(cacheName + "_rmv").getAndIncrement();
                                }
                            };
                        default:
                            throw new IgniteException(new IllegalArgumentException());
                    }
                }
            }, null, true, false);
        }
    }

    /** */
    private final static class EntryEventFilterFactory implements Factory<CacheEntryEventFilter> {
        /** Grid index to determine whether node filter has been invoked. */
        private final int idx;

        /** */
        private EntryEventFilterFactory(int idx) {
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override public CacheEntryEventFilter create() {
            return new CacheEntryEventFilter() {
                /** {@inheritDoc} */
                @Override public boolean evaluate(CacheEntryEvent evt) throws CacheEntryListenerException {
                    int evtNodeIdx = (Integer)((IgniteCacheProxy)evt.getSource()).context().grid().localNode()
                        .attributes().get("idx");
                    assertTrue(evtNodeIdx % 2 == idx % 2);
                    return true;
                }
            };
        }
    }

    /** */
    private final static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final int idx;

        /** */
        private NodeFilter(int idx) {
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return ((Integer)clusterNode.attributes().get("idx") % 2) == idx % 2;
        }
    }
}
