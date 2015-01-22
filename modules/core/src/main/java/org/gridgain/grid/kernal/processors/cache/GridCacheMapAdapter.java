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
import org.apache.ignite.cache.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Wrapper to represent cache as {@link ConcurrentMap}.
 */
public class GridCacheMapAdapter<K, V> implements ConcurrentMap<K, V> {
    /** */
    private GridCacheProjection<K, V> prj;

    /**
     * Constructor.
     *
     * @param prj Cache to wrap.
     */
    public GridCacheMapAdapter(GridCacheProjection<K, V> prj) {
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return prj.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return prj.isEmpty();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object key) {
        return prj.containsKey((K)key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean containsValue(Object value) {
        return prj.containsValue((V)value);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable
    @Override public V get(Object key) {
        try {
            return prj.get((K)key);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public V put(K key, V value) {
        try {
            return prj.put(key, value);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable
    @Override public V remove(Object key) {
        try {
            return prj.remove((K)key);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        try {
            prj.putAll(map);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public V putIfAbsent(K key, V val) {
        try {
            return prj.putIfAbsent(key, val);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public boolean remove(Object key, Object val) {
        try {
            return prj.remove((K)key, (V)val);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        try {
            return prj.replace(key, oldVal, newVal);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public V replace(K key, V val) {
        try {
            return prj.replace(key, val);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        prj.clearAll();
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        return prj.keySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<V> values() {
        return prj.values();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    @Override public Set<Entry<K, V>> entrySet() {
        return (Set<Entry<K, V>>)(Set<? extends Entry<K, V>>)prj.entrySet();
    }
}
