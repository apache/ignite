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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Cache multimap proxy
 */
public class GridCacheMultimapProxy<K, V> implements IgniteMultimap<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<T3<GridKernalContext, String, String>> stash =
        ThreadLocal.withInitial(() -> new T3<>());

    /**
     * Delegate multimap.
     */
    private GridCacheMultimapImpl<K, V> delegate;

    /**
     * Cache context.
     */
    private GridCacheContext cctx;

    /**
     * Cache gateway.
     */
    private GridCacheGateway gate;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheMultimapProxy() {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param delegate Delegate multimap.
     */
    public GridCacheMultimapProxy(GridCacheContext cctx, GridCacheMultimapImpl<K, V> delegate) {
        this.cctx = cctx;
        this.delegate = delegate;

        gate = cctx.gate();
    }

    /**
     * @return Delegate multimap.
     */
    public GridCacheMultimapImpl<K, V> delegate() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public List<V> get(K key) {
        gate.enter();

        try {
            return delegate.get(key);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key, int index) {
        gate.enter();

        try {
            return delegate.get(key, index);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public List<V> get(K key, int min, int max) {
        gate.enter();

        try {
            return delegate.get(key, min, max);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public List<V> get(K key, Iterable<Integer> indexes) {
        gate.enter();

        try {
            return delegate.get(key, indexes);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, List<V>> getAll(Collection<K> keys) {
        gate.enter();

        try {
            return delegate.getAll(keys);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Collection<K> keys, int index) {
        gate.enter();

        try {
            return delegate.getAll(keys, index);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, List<V>> getAll(Collection<K> keys, int min, int max) {
        gate.enter();

        try {
            return delegate.getAll(keys, min, max);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, List<V>> getAll(Collection<K> keys, Iterable<Integer> indexes) {
        gate.enter();

        try {
            return delegate.getAll(keys, indexes);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        gate.enter();

        try {
            delegate.clear();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        gate.enter();

        try {
            return delegate.containsKey(key);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V value) {
        gate.enter();

        try {
            return delegate.containsValue(value);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsEntry(K key, V value) {
        gate.enter();

        try {
            return delegate.containsEntry(key, value);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> entries() {
        gate.enter();

        try {
            return delegate.entries();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Set<K> localKeySet() {
        gate.enter();

        try {
            return delegate.localKeySet();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        gate.enter();

        try {
            return delegate.keySet();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean put(K key, V value) {
        gate.enter();

        try {
            return delegate.put(key, value);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putAll(K key, Iterable<? extends V> values) {
        gate.enter();

        try {
            return delegate.putAll(key, values);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putAll(IgniteMultimap<? extends K, ? extends V> multimap) {
        gate.enter();

        try {
            return delegate.putAll(multimap);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public List<V> remove(K key) {
        gate.enter();

        try {
            return delegate.remove(key);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V value) {
        gate.enter();

        try {
            return delegate.remove(key, value);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public List<V> replaceValues(K key, Iterable<? extends V> values) {
        gate.enter();

        try {
            return delegate.replaceValues(key, values);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        gate.enter();

        try {
            return delegate.isEmpty();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> iterate(int index) {
        gate.enter();

        try {
            return delegate.iterate(index);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public long size() {
        gate.enter();

        try {
            return delegate.size();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<V> values() {
        gate.enter();

        try {
            return delegate.values();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int valueCount(K key) {
        gate.enter();

        try {
            return delegate.valueCount(key);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        gate.enter();

        try {
            return delegate.name();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        gate.enter();

        try {
            delegate.close();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() {
        gate.enter();

        try {
            return delegate.collocated();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(IgniteRunnable job) {
        gate.enter();

        try {
            delegate.affinityRun(job);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(IgniteCallable<R> job) {
        gate.enter();

        try {
            return delegate.affinityCall(job);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        gate.enter();

        try {
            return delegate.removed();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx.kernalContext());
        U.writeString(out, name());
        U.writeString(out, cctx.group().name());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        T3<GridKernalContext, String, String> t = stash.get();

        t.set1((GridKernalContext)in.readObject());
        t.set2(U.readString(in));
        t.set3(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            T3<GridKernalContext, String, String> t = stash.get();

            return t.get1().dataStructures().multimap(t.get2(), t.get3(), null);
        }
        catch (IgniteCheckedException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }
}
