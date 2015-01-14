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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.util.*;

/**
 * Store implementation wrapping {@link CacheLoader} and {@link CacheWriter}.
 */
class GridCacheLoaderWriterStore<K, V> implements GridCacheStore<K, V>, LifecycleAware {
    /** */
    private final CacheLoader<K, V> ldr;

    /** */
    private final CacheWriter<K, V> writer;

    /**
     * @param ldr Loader.
     * @param writer Writer.
     */
    GridCacheLoaderWriterStore(@Nullable CacheLoader<K, V> ldr, @Nullable CacheWriter<K, V> writer) {
        assert ldr != null || writer != null;

        this.ldr = ldr;
        this.writer = writer;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ldr instanceof LifecycleAware)
            ((LifecycleAware)ldr).start();

        if (writer instanceof LifecycleAware)
            ((LifecycleAware)writer).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        if (ldr instanceof LifecycleAware)
            ((LifecycleAware)ldr).stop();

        if (writer instanceof LifecycleAware)
            ((LifecycleAware)writer).stop();
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public V load(@Nullable IgniteTx tx, K key) throws IgniteCheckedException {
        if (ldr == null)
            return null;

        return ldr.load(key);
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable IgniteTx tx, Collection<? extends K> keys, IgniteBiInClosure<K, V> c)
        throws IgniteCheckedException {
        if (ldr == null)
            return;

        Map<K, V> map = ldr.loadAll(keys);

        if (map != null) {
            for (Map.Entry<K, V> e : map.entrySet())
                c.apply(e.getKey(), e.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, K key, V val) throws IgniteCheckedException {
        if (writer == null)
            return;

        writer.write(new KeyValueEntry<>(key, val));
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable IgniteTx tx, Map<? extends K, ? extends V> map)
        throws IgniteCheckedException {
        if (writer == null)
            return;

        Collection<Cache.Entry<? extends K, ? extends V>> col =
            F.viewReadOnly(map.entrySet(), new C1<Map.Entry<? extends K, ? extends V>, Cache.Entry<? extends K, ? extends V>>() {
                @Override
                public Cache.Entry<? extends K, ? extends V> apply(Map.Entry<? extends K, ? extends V> e) {
                    return new MapEntry<>(e);
                }
            });

        writer.writeAll(col);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable IgniteTx tx, K key) throws IgniteCheckedException {
        if (writer == null)
            return;

        writer.delete(key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable IgniteTx tx, Collection<? extends K> keys) throws IgniteCheckedException {
        if (writer == null)
            return;

        writer.deleteAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
        // No-op.
    }

    /**
     *
     */
    private static class KeyValueEntry<K, V> implements Cache.Entry<K, V> {
        /** */
        private final K key;

        /** */
        private final V val;

        /**
         * @param key Key.
         * @param val Value.
         */
        KeyValueEntry(K key, V val) {
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            throw new IllegalArgumentException();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(KeyValueEntry.class, this);
        }
    }

    /**
     *
     */
    static class MapEntry<K, V> implements Cache.Entry<K, V> {
        /** */
        private final Map.Entry<K, V> e;

        /**
         * @param e Entry.
         */
        MapEntry(Map.Entry<K, V> e) {
            this.e = e;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return e.getKey();
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return e.getValue();
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            throw new IllegalArgumentException();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MapEntry.class, this);
        }
    }
}
