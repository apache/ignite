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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Config Variations query tests.
 */
public class IgniteCacheQueryConfigVariationsTest extends IgniteCacheConfigVariationsAbstractTest {
    /** */
    public static final int CNT = 50;

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testScanQuery() throws Exception {
        dataMode = DataMode.PLANE_OBJECT;

//        runInAllDataModes(new TestRunnable() {
//            @Override public void run() throws Exception {
                IgniteCache<Object, Object> cache = jcache();

                Map<Object, Object> map = new HashMap<Object, Object>(){{
                    for (int i = 0; i < CNT; i++)
                        put(key(i), value(i));
                }};

                for (Map.Entry<Object, Object> e : map.entrySet())
                    cache.put(e.getKey(), e.getValue());

                // Scan query.
                QueryCursor<Cache.Entry<Object, Object>> qry = cache.query(new ScanQuery());

                checkQueryResults(map, qry);
//            }
//        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanPartitionQuery() throws Exception {
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
            ScanQuery<Object, Object> scan = new ScanQuery<>(i);

            Collection<Cache.Entry<Object, Object>> actual = cache.query(scan).getAll();

            Map<Object, Object> exp = entries.get(i);

            assertEquals("Failed for partition: " + i, exp == null ? 0 : exp.size(), actual.size());

            if (exp != null) {
                for (Cache.Entry<Object, Object> entry : actual)
                    assertTrue(entry.getValue().equals(exp.get(entry.getKey())));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testScanFilters() throws Exception {
        IgniteCache<Object, Object> cache = jcache();

        for (int i = 0; i < CNT; i++)
            cache.put(key(i), value(i));

        QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>(new IgniteBiPredicate<Object, Object>() {
            @Override public boolean apply(Object k, Object v) {
                assertNotNull(k);
                assertNotNull(v);

                return valueOf(k) >= 20 && valueOf(v) < 40;
            }
        }));

        List<Cache.Entry<Object, Object>> list = new ArrayList<>(q.getAll());

        Collections.sort(list, new Comparator<Cache.Entry<Object, Object>>() {
            @Override public int compare(Cache.Entry<Object, Object> e1, Cache.Entry<Object, Object> e2) {
                return valueOf(e1.getKey()) - valueOf(e2.getKey());
            }
        });

        for (int i = 20; i < 40; i++) {
            Cache.Entry<Object, Object> e = list.get(i - 20);

            assertEquals(i, valueOf(e.getKey()));
            assertEquals(i, valueOf(e.getValue()));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testLocalScanQuery() throws Exception {
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

        QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>().setLocal(true));

        checkQueryResults(map, q);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testScanQueryLocalFilter() throws Exception {
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

        QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>(filter).setLocal(true));

        checkQueryResults(map, q);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("SubtractionInCompareTo")
    public void testScanQueryPartitionFilter() throws Exception {
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
            info(">>>>> part=" + part);

            QueryCursor<Cache.Entry<Object, Object>> q = cache.query(new ScanQuery<>(part, filter));

            Map<Object, Object> expMap = partMap.get(part);

            checkQueryResults(expMap == null ? Collections.emptyMap() : expMap, q);
        }
    }

    /**
     * @param expMap Expected map.
     * @param cursor Query cursor.
     */
    private void checkQueryResults(Map<Object, Object> expMap, QueryCursor<Cache.Entry<Object, Object>> cursor) {
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
    }
}
