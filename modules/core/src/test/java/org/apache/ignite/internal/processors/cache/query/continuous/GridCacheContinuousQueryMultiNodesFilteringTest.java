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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
@SuppressWarnings("unchecked")
public class GridCacheContinuousQueryMultiNodesFilteringTest extends GridCommonAbstractTest {
    /** */
    private static final int SERVER_GRIDS_COUNT = 6;

    /** */
    public static final int KEYS = 2_000;

    /** Cache entry operations' counts. */
    private static final ConcurrentMap<String, AtomicInteger> opCounts = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testFiltersAndListeners() throws Exception {
        for (int i = 1; i <= SERVER_GRIDS_COUNT; i++)
            startGrid(i, false);

        startGrid(SERVER_GRIDS_COUNT + 1, true);

        for (int i = 1; i <= SERVER_GRIDS_COUNT + 1; i++) {
            for (int j = 0; j < i; j++) {
                jcache(i, "part" + i).put("k" + j, "v0");
                jcache(i, "repl" + i).put("k" + j, "v0");

                // Should trigger updates
                jcache(i, "part" + i).put("k" + j, "v1");
                jcache(i, "repl" + i).put("k" + j, "v1");

                jcache(i, "part" + i).remove("k" + j);
                jcache(i, "repl" + i).remove("k" + j);
            }
        }

        for (int i = 1; i <= SERVER_GRIDS_COUNT + 1; i++) {
            // For each i, we did 3 ops on 2 caches on i keys, hence expected number.
            final int expTotal = i * 3 * 2;
            final int i0 = i;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return opCounts.get("qry" + i0 + "_total").get() == expTotal;
                }
            }, 5000);

            int partInserts = opCounts.get("part" + i + "_ins").get();
            int replInserts = opCounts.get("repl" + i + "_ins").get();
            int partUpdates = opCounts.get("part" + i + "_upd").get();
            int replUpdates = opCounts.get("repl" + i + "_upd").get();
            int partRemoves = opCounts.get("part" + i + "_rmv").get();
            int replRemoves = opCounts.get("repl" + i + "_rmv").get();
            int totalQryOps = opCounts.get("qry" + i + "_total").get();

            assertEquals(i, partInserts);
            assertEquals(i, replInserts);

            assertEquals(i, partUpdates);
            assertEquals(i, replUpdates);

            assertEquals(i, partRemoves);
            assertEquals(i, replRemoves);

            assertEquals(expTotal, totalQryOps);

            assertEquals(totalQryOps, partInserts + replInserts + partUpdates + replUpdates + partRemoves + replRemoves);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithNodeFilter() throws Exception {
        List<QueryCursor> qryCursors = new ArrayList<>();

        final int nodesCnt = 3;

        startGridsMultiThreaded(nodesCnt);

        awaitPartitionMapExchange();

        CacheConfiguration ccfg = cacheConfiguration(new NodeFilterByRegexp(".*(0|1)$"));

        grid(0).createCache(ccfg);

        final AtomicInteger cntr = new AtomicInteger();

        final ConcurrentMap<ClusterNode, Set<Integer>> maps = new ConcurrentHashMap<>();

        final AtomicBoolean doubleNtfFail = new AtomicBoolean(false);

        CacheEntryUpdatedListener<Integer, Integer> lsnr = new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts)
                throws CacheEntryListenerException {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    cntr.incrementAndGet();

                    ClusterNode node = ((Ignite)e.getSource().unwrap(Ignite.class)).cluster().localNode();

                    Set<Integer> set = maps.get(node);

                    if (set == null) {
                        set = new ConcurrentSkipListSet<>();

                        Set<Integer> oldVal = maps.putIfAbsent(node, set);

                        set = oldVal != null ? oldVal : set;
                    }

                    if (!set.add(e.getValue()))
                        doubleNtfFail.set(false);
                }
            }
        };

        for (int i = 0; i < nodesCnt; i++) {
            ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

            qry.setLocalListener(lsnr);

            Ignite ignite = grid(i);

            log.info("Try to start CQ on node: " + ignite.cluster().localNode().id());

            qryCursors.add(ignite.cache(ccfg.getName()).query(qry));

            log.info("CQ started on node: " + ignite.cluster().localNode().id());
        }

        startClientGrid(nodesCnt);

        awaitPartitionMapExchange();

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        qryCursors.add(grid(nodesCnt).cache(ccfg.getName()).query(qry));

        for (int i = 0; i <= nodesCnt; i++) {
            for (int key = 0; key < KEYS; key++) {
                int val = (i * KEYS) + key;

                grid(i).cache(ccfg.getName()).put(val, val);
            }
        }

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return cntr.get() >= 2 * (nodesCnt + 1) * KEYS;
            }
        }, 5000L));

        assertFalse("Got duplicate", doubleNtfFail.get());

        for (int i = 0; i < (nodesCnt + 1) * KEYS; i++) {
            for (Map.Entry<ClusterNode, Set<Integer>> e : maps.entrySet())
                assertTrue("Lost event on node: " + e.getKey().id() + ", event: " + i, e.getValue().remove(i));
        }

        for (Map.Entry<ClusterNode, Set<Integer>> e : maps.entrySet())
            assertTrue("Unexpected event on node: " + e.getKey(), e.getValue().isEmpty());

        assertEquals("Not expected count of CQ", nodesCnt + 1, qryCursors.size());

        for (QueryCursor cur : qryCursors)
            cur.close();
    }

    /** */
    private Ignite startGrid(final int idx, boolean isClientMode) throws Exception {
        String igniteInstanceName = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = optimize(getConfiguration(igniteInstanceName)).setClientMode(isClientMode);

        cfg.setUserAttributes(Collections.singletonMap("idx", idx));

        Ignite node = startGrid(igniteInstanceName, cfg);

        IgnitePredicate<ClusterNode> nodeFilter = new NodeFilter(idx);

        String partCacheName = "part" + idx;

        IgniteCache partCache = node.createCache(defaultCacheConfiguration().setName("part" + idx)
            .setCacheMode(PARTITIONED).setBackups(1).setNodeFilter(nodeFilter));

        opCounts.put(partCacheName + "_ins", new AtomicInteger());
        opCounts.put(partCacheName + "_upd", new AtomicInteger());
        opCounts.put(partCacheName + "_rmv", new AtomicInteger());

        partCache.registerCacheEntryListener(new ListenerConfiguration(partCacheName, ListenerConfiguration.Op.INSERT));
        partCache.registerCacheEntryListener(new ListenerConfiguration(partCacheName, ListenerConfiguration.Op.UPDATE));
        partCache.registerCacheEntryListener(new ListenerConfiguration(partCacheName, ListenerConfiguration.Op.REMOVE));

        String replCacheName = "repl" + idx;

        IgniteCache replCache = node.createCache(defaultCacheConfiguration().setName("repl" + idx)
            .setCacheMode(REPLICATED).setNodeFilter(nodeFilter));

        opCounts.put(replCacheName + "_ins", new AtomicInteger());
        opCounts.put(replCacheName + "_upd", new AtomicInteger());
        opCounts.put(replCacheName + "_rmv", new AtomicInteger());

        replCache.registerCacheEntryListener(new ListenerConfiguration(replCacheName, ListenerConfiguration.Op.INSERT));
        replCache.registerCacheEntryListener(new ListenerConfiguration(replCacheName, ListenerConfiguration.Op.UPDATE));
        replCache.registerCacheEntryListener(new ListenerConfiguration(replCacheName, ListenerConfiguration.Op.REMOVE));

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

    /**
     * @param filter Node filter.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(NodeFilterByRegexp filter) {
        return new CacheConfiguration("test-cache-cq")
            .setBackups(1)
            .setNodeFilter(filter)
            .setAtomicityMode(atomicityMode())
            .setWriteSynchronizationMode(FULL_SYNC)
            .setCacheMode(PARTITIONED);
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** */
    private static final class ListenerConfiguration extends MutableCacheEntryListenerConfiguration {
        /** Operation. */
        enum Op {
            /** Insert. */
            INSERT,

            /** Update. */
            UPDATE,

            /** Remove. */
            REMOVE
        }

        /** */
        ListenerConfiguration(final String cacheName, final Op op) {
            super(new Factory<CacheEntryListener>() {
                /** {@inheritDoc} */
                @Override public CacheEntryListener create() {
                    switch (op) {
                        case INSERT:
                            return new CacheEntryCreatedListener() {
                                /** {@inheritDoc} */
                                @Override public void onCreated(Iterable iterable) {
                                    for (Object evt : iterable)
                                        opCounts.get(cacheName + "_ins").getAndIncrement();
                                }
                            };
                        case UPDATE:
                            return new CacheEntryUpdatedListener() {
                                /** {@inheritDoc} */
                                @Override public void onUpdated(Iterable iterable) {
                                    for (Object evt : iterable)
                                        opCounts.get(cacheName + "_upd").getAndIncrement();
                                }
                            };
                        case REMOVE:
                            return new CacheEntryRemovedListener() {
                                /** {@inheritDoc} */
                                @Override public void onRemoved(Iterable iterable) {
                                    for (Object evt : iterable)
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
    private static final class EntryEventFilterFactory implements Factory<CacheEntryEventFilter> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

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
                    int evtNodeIdx = (Integer)(ignite.cluster().localNode().attributes().get("idx"));

                    assertTrue(evtNodeIdx % 2 == idx % 2);

                    return true;
                }
            };
        }
    }

    /** */
    private static final class NodeFilter implements IgnitePredicate<ClusterNode> {
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

    /** */
    private static final class NodeFilterByRegexp implements IgnitePredicate<ClusterNode> {
        /** */
        private final Pattern pattern;

        /** */
        private NodeFilterByRegexp(String regExp) {
            this.pattern = Pattern.compile(regExp);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return pattern.matcher(clusterNode.id().toString()).matches();
        }
    }
}
