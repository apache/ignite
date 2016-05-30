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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.query.CacheQueryType.SCAN;

/**
 * Config Variations query tests.
 */
public class IgniteCacheConfigVariationsQueryTest extends IgniteCacheConfigVariationsAbstractTest {
    /** */
    public static final int CNT = 50;

    /** */
    private Map<Object, Object> evtMap;

    /** */
    private CountDownLatch readEvtLatch;

    /** */
    private CountDownLatch execEvtLatch;

    /** */
    private IgnitePredicate[] objReadLsnrs;

    /** */
    private IgnitePredicate[] qryExecLsnrs;

    /** */
    private Map<Object, Object> expMap;

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testScanQuery() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                try {
                    IgniteCache<Object, Object> cache = jcache();

                    Map<Object, Object> map = new HashMap<Object, Object>() {{
                        for (int i = 0; i < CNT; i++)
                            put(key(i), value(i));
                    }};

                    registerEventListeners(map);

                    for (Map.Entry<Object, Object> e : map.entrySet())
                        cache.put(e.getKey(), e.getValue());

                    // Scan query.
                    QueryCursor<Cache.Entry<Object, Object>> qry = cache.query(new ScanQuery());

                    checkQueryResults(map, qry);
                }
                finally {
                    stopListeners();
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanPartitionQuery() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                IgniteCache<Object, Object> cache = jcache();

                GridCacheContext cctx = ((IgniteCacheProxy)cache).context();

                Map<Integer, Map<Object, Object>> entries = new HashMap<>();

                for (int i = 0; i < CNT; i++) {
                    Object key = key(i);
                    Object val = value(i);

                    cache.put(key, val);

                    int part = cctx.affinity().partition(key);

                    Map<Object, Object> partEntries = entries.get(part);

                    if (partEntries == null)
                        entries.put(part, partEntries = new HashMap<>());

                    partEntries.put(key, val);
                }

                for (int i = 0; i < cctx.affinity().partitions(); i++) {
                    try {
                        Map<Object, Object> exp = entries.get(i);

                        if (exp == null)
                            System.out.println();

                        registerEventListeners(exp);

                        ScanQuery<Object, Object> scan = new ScanQuery<>(i);

                        Collection<Cache.Entry<Object, Object>> actual = cache.query(scan).getAll();

                        assertEquals("Failed for partition: " + i, exp == null ? 0 : exp.size(), actual.size());

                        if (exp != null) {
                            for (Cache.Entry<Object, Object> entry : actual)
                                assertTrue(entry.getValue().equals(exp.get(entry.getKey())));
                        }

                        checkEvents();
                    }
                    finally {
                        stopListeners();
                    }
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testScanFilters() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                try {
                    IgniteCache<Object, Object> cache = jcache();

                    IgniteBiPredicate<Object, Object> p = new IgniteBiPredicate<Object, Object>() {
                        @Override public boolean apply(Object k, Object v) {
                            assertNotNull(k);
                            assertNotNull(v);

                            return valueOf(k) >= 20 && valueOf(v) < 40;
                        }
                    };

                    Map<Object, Object> exp = new HashMap<>();

                    for (int i = 0; i < CNT; i++) {
                        Object key = key(i);
                        Object val = value(i);

                        cache.put(key, val);

                        if (p.apply(key, val))
                            exp.put(key, val);
                    }

                    registerEventListeners(exp, true);

                    QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>(p));

                    checkQueryResults(exp, q);
                }
                finally {
                    stopListeners();
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testLocalScanQuery() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                try {
                    IgniteCache<Object, Object> cache = jcache();

                    ClusterNode locNode = testedGrid().cluster().localNode();
                    Affinity<Object> affinity = testedGrid().affinity(cacheName());

                    Map<Object, Object> map = new HashMap<>();

                    for (int i = 0; i < CNT; i++) {
                        Object key = key(i);
                        Object val = value(i);

                        cache.put(key, val);

                        if (!isClientMode() && (cacheMode() == REPLICATED || affinity.isPrimary(locNode, key)))
                            map.put(key, val);
                    }

                    registerEventListeners(map);

                    QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>().setLocal(true));

                    checkQueryResults(map, q);
                }
                finally {
                    stopListeners();
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testScanQueryLocalFilter() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                try {
                    IgniteCache<Object, Object> cache = jcache();

                    ClusterNode locNode = testedGrid().cluster().localNode();

                    Map<Object, Object> map = new HashMap<>();

                    IgniteBiPredicate<Object, Object> filter = new IgniteBiPredicate<Object, Object>() {
                        @Override public boolean apply(Object k, Object v) {
                            assertNotNull(k);
                            assertNotNull(v);

                            return valueOf(k) >= 20 && valueOf(v) < 40;
                        }
                    };

                    for (int i = 0; i < CNT; i++) {
                        Object key = key(i);
                        Object val = value(i);

                        cache.put(key, val);

                        if (!isClientMode() && (cacheMode() == REPLICATED
                            || testedGrid().affinity(cacheName()).isPrimary(locNode, key)) && filter.apply(key, val))
                            map.put(key, val);
                    }

                    registerEventListeners(map, true);

                    QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>(filter).setLocal(true));

                    checkQueryResults(map, q);
                }
                finally {
                    stopListeners();
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testScanQueryPartitionFilter() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                IgniteCache<Object, Object> cache = jcache();

                Affinity<Object> affinity = testedGrid().affinity(cacheName());

                Map<Integer, Map<Object, Object>> partMap = new HashMap<>();

                IgniteBiPredicate<Object, Object> filter = new IgniteBiPredicate<Object, Object>() {
                    @Override public boolean apply(Object k, Object v) {
                        assertNotNull(k);
                        assertNotNull(v);

                        return valueOf(k) >= 20 && valueOf(v) < 40;
                    }
                };

                for (int i = 0; i < CNT; i++) {
                    Object key = key(i);
                    Object val = value(i);

                    cache.put(key, val);

                    if (filter.apply(key, val)) {
                        int part = affinity.partition(key);

                        Map<Object, Object> map = partMap.get(part);

                        if (map == null)
                            partMap.put(part, map = new HashMap<>());

                        map.put(key, val);
                    }
                }

                for (int part = 0; part < affinity.partitions(); part++) {
                    try {
                        Map<Object, Object> expMap = partMap.get(part);

                        expMap = expMap == null ? Collections.emptyMap() : expMap;

                        registerEventListeners(expMap, true);

                        QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>(part, filter));

                        checkQueryResults(expMap, q);
                    }
                    finally {
                        stopListeners();
                    }
                }
            }
        });
    }

    /**
     * @param expMap Expected map.
     * @param cursor Query cursor.
     */
    private void checkQueryResults(Map<Object, Object> expMap, QueryCursor<Cache.Entry<Object, Object>> cursor)
        throws InterruptedException {
        Iterator<Cache.Entry<Object, Object>> iter = cursor.iterator();

        try {
            assertNotNull(iter);

            int cnt = 0;

            while (iter.hasNext()) {
                Cache.Entry<Object, Object> e = iter.next();

                assertNotNull(e.getKey());
                assertNotNull(e.getValue());

                Object expVal = expMap.get(e.getKey());

                assertNotNull("Failed to resolve expected value for key: " + e.getKey(), expVal);

                assertEquals(expVal, e.getValue());

                cnt++;
            }

            assertEquals(expMap.size(), cnt);
        }
        finally {
            cursor.close();
        }

        checkEvents();
    }

    /**
     * Registers event listeners.
     * @param expMap Expected read events count.
     */
    private void registerEventListeners(Map<Object, Object> expMap) {
        registerEventListeners(expMap, false);
    }

    /**
     * Registers event listeners.
     * @param expMap Expected read events count.
     * @param filterExp Scan query uses filter.
     */
    private void registerEventListeners(Map<Object, Object> expMap, final boolean filterExp) {
        this.expMap = expMap != null ? expMap : Collections.emptyMap();

        Set<ClusterNode> affNodes= new HashSet<>();

        if (cacheMode() != REPLICATED) {
            Affinity<Object> aff = testedGrid().affinity(cacheName());

            for (Object key : this.expMap.keySet())
                affNodes.add(aff.mapKeyToNode(key));
        }

        int execEvtCnt = cacheMode() == REPLICATED || (cacheMode() == PARTITIONED && affNodes.isEmpty()) ? 1 : affNodes.size();

        evtMap = new ConcurrentHashMap<>();
        readEvtLatch = new CountDownLatch(this.expMap.size());
        execEvtLatch = new CountDownLatch(execEvtCnt);

        objReadLsnrs = new IgnitePredicate[gridCount()];
        qryExecLsnrs = new IgnitePredicate[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            IgnitePredicate<Event> pred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assertTrue("Event: " + evt, evt instanceof CacheQueryReadEvent);

                    CacheQueryReadEvent<Object, Object> qe = (CacheQueryReadEvent<Object, Object>)evt;

                    assertEquals(SCAN.name(), qe.queryType());
                    assertEquals(cacheName(), qe.cacheName());

                    assertNull(qe.className());
                    assertNull(qe.clause());
                    assertEquals(filterExp, qe.scanQueryFilter() != null);
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    evtMap.put(qe.key(), qe.value());

                    assertFalse(readEvtLatch.getCount() == 0);

                    readEvtLatch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(pred, EVT_CACHE_QUERY_OBJECT_READ);

            objReadLsnrs[i] = pred;

            IgnitePredicate<Event> execPred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assertTrue("Event: " + evt, evt instanceof CacheQueryExecutedEvent);

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertEquals(SCAN.name(), qe.queryType());
                    assertEquals(cacheName(), qe.cacheName());

                    assertNull(qe.className());
                    assertNull(qe.clause());
                    assertEquals(filterExp, qe.scanQueryFilter() != null);
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    assertFalse("Too many events.", execEvtLatch.getCount() == 0);

                    execEvtLatch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(execPred, EVT_CACHE_QUERY_EXECUTED);

            qryExecLsnrs[i] = execPred;
        }
    }

    /**
     * Stops listenening.
     */
    private void stopListeners() {
        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().stopLocalListen(objReadLsnrs[i]);
            grid(i).events().stopLocalListen(qryExecLsnrs[i]);
        }
    }

    /**
     * @throws InterruptedException If failed.
     */
    private void checkEvents() throws InterruptedException {
        assertTrue(execEvtLatch.await(1000, MILLISECONDS));
        assertTrue(readEvtLatch.await(1000, MILLISECONDS));

        assertEquals(expMap.size(), evtMap.size());

        for (Map.Entry<Object, Object> e : expMap.entrySet())
            assertEquals(e.getValue(), evtMap.get(e.getKey()));
    }
}
