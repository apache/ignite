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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jsr166.ConcurrentHashMap8;

/**
 * {@link TestCacheStoreStrategy} implemented as a wrapper around {@link #map}
 */
public class MapCacheStoreStrategy implements TestCacheStoreStrategy {
    /** Removes counter. */
    private final static AtomicInteger removes = new AtomicInteger();

    /** Writes counter. */
    private final static AtomicInteger writes = new AtomicInteger();

    /** Reads counter. */
    private final static AtomicInteger reads = new AtomicInteger();

    /** Store map. */
    private final static Map<Object, Object> map = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public int getReads() {
        return reads.get();
    }

    /** {@inheritDoc} */
    @Override public int getWrites() {
        return writes.get();
    }

    /** {@inheritDoc} */
    @Override public int getRemoves() {
        return removes.get();
    }

    /** {@inheritDoc} */
    @Override public int getStoreSize() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public void resetStore() {
        map.clear();

        reads.set(0);
        writes.set(0);
        removes.set(0);
    }

    /** {@inheritDoc} */
    @Override public void putToStore(Object key, Object val) {
        map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAllToStore(Map<?, ?> data) {
        map.putAll(data);
    }

    /** {@inheritDoc} */
    @Override public Object getFromStore(Object key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void removeFromStore(Object key) {
        map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public boolean isInStore(Object key) {
        return map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public void updateCacheConfiguration(CacheConfiguration<Object, Object> cfg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Factory<? extends CacheStore<Object, Object>> getStoreFactory() {
        return FactoryBuilder.factoryOf(MapCacheStore.class);
    }

    /** Serializable {@link #map} backed cache store factory */
    public static class MapStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            return new MapCacheStore();
        }
    }

    /** {@link CacheStore} backed by {@link #map} */
    public static class MapCacheStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry<Object, Object> e : map.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            reads.incrementAndGet();
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> e) {
            writes.incrementAndGet();
            map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            removes.incrementAndGet();
            map.remove(key);
        }
    }
}
