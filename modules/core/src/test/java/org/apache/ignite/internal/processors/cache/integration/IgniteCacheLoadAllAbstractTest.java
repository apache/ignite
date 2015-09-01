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

package org.apache.ignite.internal.processors.cache.integration;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.integration.CompletionListenerFuture;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.jsr166.ConcurrentHashMap8;

/**
 * Test for {@link Cache#loadAll(Set, boolean, CompletionListener)}.
 */
public abstract class IgniteCacheLoadAllAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private volatile boolean writeThrough = true;

    /** */
    private static ConcurrentHashMap8<Object, Object> storeMap;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setWriteThrough(writeThrough);

        ccfg.setCacheLoaderFactory(new CacheLoaderFactory());

        ccfg.setCacheWriterFactory(new CacheWriterFactory());

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        storeMap = new ConcurrentHashMap8<>();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        storeMap = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadAll() throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(0);

        // Put some data in cache, it also should be put in store.

        final int KEYS = 10;

        for (int i = 0; i < KEYS; i++)
            cache0.put(i, String.valueOf(i));

        // Restart nodes with write-through disabled so that data in store does not change.

        stopAllGrids();

        writeThrough = false;

        startGrids();

        cache0 = jcache(0);

        Set<Integer> keys = new HashSet<>();

        Map<Integer, String> expVals = new HashMap<>();

        for (int i = 0; i < KEYS / 2; i++) {
            keys.add(i);

            expVals.put(i, String.valueOf(i));
        }

        for (int i = KEYS + 1000; i < KEYS + 1010; i++)
            keys.add(i);

        CompletionListenerFuture fut = new CompletionListenerFuture();

        log.info("Load1.");

        cache0.loadAll(keys, false, fut);

        fut.get();

        checkValues(KEYS, expVals);

        HashMap<Integer, String> expChangedVals = new HashMap<>();

        for (int i = 0; i < KEYS / 2; i++) {
            String val = "changed";

            cache0.put(i, val);

            expChangedVals.put(i, val);
        }

        checkValues(KEYS, expChangedVals);

        fut = new CompletionListenerFuture();

        log.info("Load2.");

        cache0.loadAll(keys, false, fut);

        fut.get();

        checkValues(KEYS, expChangedVals);

        log.info("Load3.");

        fut = new CompletionListenerFuture();

        cache0.loadAll(keys, true, fut);

        fut.get();

        checkValues(KEYS, expVals);

        for (int i = 0; i < KEYS; i++) {
            keys.add(i);

            expVals.put(i, String.valueOf(i));
        }

        fut = new CompletionListenerFuture();

        log.info("Load4.");

        cache0.loadAll(keys, false, fut);

        fut.get();

        checkValues(KEYS, expVals);
    }

    /**
     * @param keys Keys to check.
     * @param expVals Expected values.
     */
    private void checkValues(int keys, Map<Integer, String> expVals) {
        Affinity<Object> aff = grid(0).affinity(null);

        for (int i = 0; i < gridCount(); i++) {
            ClusterNode node = ignite(i).cluster().localNode();

            IgniteCache<Integer, String> cache = jcache(i);

            for (int key = 0; key < keys; key++) {
                String expVal = expVals.get(key);

                if (aff.isPrimaryOrBackup(node, key)) {
                    assertEquals(expVal, cache.localPeek(key, CachePeekMode.ONHEAP));

                    assertEquals(expVal, cache.get(key));
                }
                else {
                    assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                    if (!expVals.containsKey(key))
                        assertNull(cache.get(key));
                }
            }

            for (int key = keys + 1000; i < keys + 1010; i++) {
                assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                assertNull(cache.get(key));
            }
        }
    }

    /**
     *
     */
    private static class CacheLoaderFactory implements Factory<CacheLoader> {
        @Override public CacheLoader create() {
            return new CacheLoader<Object, Object>() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                @Override public Map<Object, Object> loadAll(Iterable<?> keys) throws CacheLoaderException {
                    Map<Object, Object> loaded = new HashMap<>();

                    for (Object key : keys) {
                        Object val = storeMap.get(key);

                        if (val != null)
                            loaded.put(key, val);
                    }

                    return loaded;
                }
            };
        }
    }

    /**
     *
     */
    private static class CacheWriterFactory implements Factory<CacheWriter> {
        @Override public CacheWriter create() {
            return new CacheWriter<Object, Object>() {
                @Override public void write(Cache.Entry<?, ?> e) {
                    storeMap.put(e.getKey(), e.getValue());
                }

                @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) {
                    for (Cache.Entry<?, ?> e : entries)
                        write(e);
                }

                @Override public void delete(Object key) {
                    storeMap.remove(key);
                }

                @Override public void deleteAll(Collection<?> keys) {
                    for (Object key : keys)
                        delete(key);
                }
            };
        }
    }
}