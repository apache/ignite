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

package org.apache.ignite.ml.math.impls.storage.vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.ValueMapper;
import org.apache.ignite.ml.math.VectorKeyMapper;
import org.apache.ignite.ml.math.VectorStorage;

/**
 * Vector storage based on existing cache and index and value mapping functions.
 */
public class CacheVectorStorage<K, V> implements VectorStorage {
    /** Storage size. */
    private int size;
    /** Key mapper. */
    private VectorKeyMapper<K> keyMapper;
    /** Value mapper. */
    private ValueMapper<V> valMapper;
    /** Underlying ignite cache. */
    private IgniteCache<K, V> cache;

    /**
     *
     */
    public CacheVectorStorage() {
        // No-op.
    }

    /**
     * @param size
     * @param cache
     * @param keyMapper
     * @param valMapper
     */
    public CacheVectorStorage(int size, IgniteCache<K, V> cache, VectorKeyMapper<K> keyMapper,
        ValueMapper<V> valMapper) {
        assert size > 0;
        assert cache != null;
        assert keyMapper != null;
        assert valMapper != null;

        this.size = size;
        this.cache = cache;
        this.keyMapper = keyMapper;
        this.valMapper = valMapper;
    }

    /**
     *
     *
     */
    public IgniteCache<K, V> cache() {
        return cache;
    }

    /**
     *
     *
     */
    public VectorKeyMapper<K> keyMapper() {
        return keyMapper;
    }

    /**
     *
     *
     */
    public ValueMapper<V> valueMapper() {
        return valMapper;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return valMapper.toDouble(cache.get(keyMapper.apply(i)));
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        cache.put(keyMapper.apply(i), valMapper.fromDouble(v));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeObject(keyMapper);
        out.writeObject(valMapper);
        out.writeUTF(cache.getName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        keyMapper = (VectorKeyMapper<K>)in.readObject();
        valMapper = (ValueMapper<V>)in.readObject();
        cache = Ignition.localIgnite().getOrCreateCache(in.readUTF());
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + size();
        res = res * 37 + keyMapper.hashCode();
        res = res * 37 + valMapper.hashCode();
        res = res * 37 + cache.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        CacheVectorStorage that = (CacheVectorStorage)obj;

        return size == that.size
            && (keyMapper != null ? keyMapper.getClass().equals(that.keyMapper.getClass()) : that.keyMapper == null)
            && (valMapper != null ? valMapper.getClass().equals(that.valMapper.getClass()) : that.valMapper == null)
            && (cache != null ? cache.equals(that.cache) : that.cache == null);
    }
}
