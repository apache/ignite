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
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;

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
                QueryCursor<Cache.Entry<Object, Object>> qry = cache.query(new ScanQuery<Object, Object>());

                Iterator<Cache.Entry<Object, Object>> iter = qry.iterator();

                assert iter != null;

                int cnt = 0;

                while (iter.hasNext()) {
                    Cache.Entry<Object, Object> e = iter.next();

                    assertNotNull(e.getKey());
                    assertNotNull(e.getValue());

                    Object expVal = map.get(e.getKey());

                    assertNotNull("Failed to resolve expected value for key: " + e.getKey(), expVal);

                    assertEquals(expVal, e.getValue());

                    cnt++;
                }

                assertEquals(map.size(), cnt);
//            }
//        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanPartirionQuery() throws Exception {
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
}
