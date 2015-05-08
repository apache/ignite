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

import org.apache.ignite.cache.store.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.io.*;
import java.util.*;

/**
 * Store implementation wrapping {@link CacheLoader} and {@link CacheWriter}.
 */
class GridCacheLoaderWriterStore<K, V> implements CacheStore<K, V>, LifecycleAware, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

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

    /**
     * @return Cache loader.
     */
    CacheLoader<K, V> loader() {
        return ldr;
    }

    /**
     * @return Cache writer.
     */
    CacheWriter<K, V> writer() {
        return writer;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (ldr instanceof LifecycleAware)
            ((LifecycleAware)ldr).start();

        if (writer instanceof LifecycleAware)
            ((LifecycleAware)writer).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
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
    @Nullable @Override public V load(K key) {
        if (ldr == null)
            return null;

        return ldr.load(key);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) {
        if (ldr == null)
            return Collections.emptyMap();

        return ldr.loadAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) {
        if (writer == null)
            return;

        writer.write(entry);
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        if (writer == null)
            return;

        writer.writeAll(entries);
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        if (writer == null)
            return;

        writer.delete(key);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) {
        if (writer == null)
            return;

        writer.deleteAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) {
        // No-op.
    }
}
