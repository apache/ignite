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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.stream.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Bundled factory for cache updaters.
 */
public class DataStreamerCacheUpdaters {
    /** */
    private static final StreamReceiver INDIVIDUAL = new Individual();

    /** */
    private static final StreamReceiver BATCHED = new Batched();

    /** */
    private static final StreamReceiver BATCHED_SORTED = new BatchedSorted();

    /**
     * Updates cache using independent {@link org.apache.ignite.internal.processors.cache.GridCache#put(Object, Object, CacheEntryPredicate[])} and
     * {@link org.apache.ignite.internal.processors.cache.GridCache#remove(Object, CacheEntryPredicate[])} operations. Thus it is safe from deadlocks but performance
     * is not the best.
     *
     * @return Single updater.
     */
    public static <K, V> StreamReceiver<K, V> individual() {
        return INDIVIDUAL;
    }

    /**
     * Updates cache using batched methods {@link org.apache.ignite.internal.processors.cache.GridCache#putAll(Map, CacheEntryPredicate[])} and
     * {@link org.apache.ignite.internal.processors.cache.GridCache#removeAll(Collection, CacheEntryPredicate[])}. Can cause deadlocks if the same keys are getting
     * updated concurrently. Performance is generally better than in {@link #individual()}.
     *
     * @return Batched updater.
     */
    public static <K, V> StreamReceiver<K, V> batched() {
        return BATCHED;
    }

    /**
     * Updates cache using batched methods {@link org.apache.ignite.internal.processors.cache.GridCache#putAll(Map, CacheEntryPredicate[])} and
     * {@link org.apache.ignite.internal.processors.cache.GridCache#removeAll(Collection, CacheEntryPredicate[])}. Keys are sorted in natural order and if all updates
     * use the same rule deadlock can not happen. Performance is generally better than in {@link #individual()}.
     *
     * @return Batched sorted updater.
     */
    public static <K extends Comparable<?>, V> StreamReceiver<K, V> batchedSorted() {
        return BATCHED_SORTED;
    }

    /**
     * Updates cache.
     *
     * @param cache Cache.
     * @param rmvCol Keys to remove.
     * @param putMap Entries to put.
     * @throws IgniteException If failed.
     */
    protected static <K, V> void updateAll(IgniteCache<K, V> cache, @Nullable Set<K> rmvCol,
        Map<K, V> putMap) {
        assert rmvCol != null || putMap != null;

        // Here we assume that there are no key duplicates, so the following calls are valid.
        if (rmvCol != null)
            ((IgniteCacheProxy<K, V>)cache).removeAll(rmvCol);

        if (putMap != null)
            cache.putAll(putMap);
    }

    /**
     * Simple cache updater implementation. Updates keys one by one thus is not dead lock prone.
     */
    private static class Individual<K, V> implements StreamReceiver<K, V>, InternalUpdater {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) {
            assert cache != null;
            assert !F.isEmpty(entries);

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key != null;

                V val = entry.getValue();

                if (val == null)
                    cache.remove(key);
                else
                    cache.put(key, val);
            }
        }
    }

    /**
     * Batched updater. Updates cache using batch operations thus is dead lock prone.
     */
    private static class Batched<K, V> implements StreamReceiver<K, V>, InternalUpdater {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) {
            assert cache != null;
            assert !F.isEmpty(entries);

            Map<K, V> putAll = null;
            Set<K> rmvAll = null;

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key != null;

                V val = entry.getValue();

                if (val == null) {
                    if (rmvAll == null)
                        rmvAll = new HashSet<>();

                    rmvAll.add(key);
                }
                else {
                    if (putAll == null)
                        putAll = new HashMap<>();

                    putAll.put(key, val);
                }
            }

            updateAll(cache, rmvAll, putAll);
        }
    }

    /**
     * Batched updater. Updates cache using batch operations thus is dead lock prone.
     */
    private static class BatchedSorted<K, V> implements StreamReceiver<K, V>, InternalUpdater {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) {
            assert cache != null;
            assert !F.isEmpty(entries);

            Map<K, V> putAll = null;
            Set<K> rmvAll = null;

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key instanceof Comparable;

                V val = entry.getValue();

                if (val == null) {
                    if (rmvAll == null)
                        rmvAll = new TreeSet<>();

                    rmvAll.add(key);
                }
                else {
                    if (putAll == null)
                        putAll = new TreeMap<>();

                    putAll.put(key, val);
                }
            }

            updateAll(cache, rmvAll, putAll);
        }
    }

    /**
     * Marker interface for updaters which do not need to unwrap cache objects.
     */
    public static interface InternalUpdater {
        // No-op.
    }
}
