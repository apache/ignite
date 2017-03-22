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

package org.apache.ignite.math.impls.storage.vector;

import org.apache.ignite.*;
import org.apache.ignite.math.*;
import java.io.*;

/**
 * Vector storage based on existing cache and index and value mapping functions.
 */
public class CacheVectorStorage<K, V> implements VectorStorage {
    private int size;
    private VectorKeyMapper<K> keyMapper;
    private ValueMapper<V> valMapper;
    private IgniteCache<K, V> cache;

    /**
     *
     */
    public CacheVectorStorage() {
        // No-op.
    }

    /**
     * 
     * @param size
     * @param cache
     * @param keyMapper
     * @param valMapper
     */
    public CacheVectorStorage(int size, IgniteCache<K, V> cache, VectorKeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
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
     * @return
     */
    public IgniteCache<K, V> cache() {
        return cache;
    }

    /**
     *
     * @return
     */
    public VectorKeyMapper<K> keyMapper() {
        return keyMapper;
    }

    /**
     *
     * @return
     */
    public ValueMapper<V> valueMapper() {
        return valMapper;
    }

    @Override public int size() {
        return size;
    }

    @Override public double get(int i) {
        return valMapper.toDouble(cache.get(keyMapper.apply(i)));
    }

    @Override public void set(int i, double v) {
        cache.put(keyMapper.apply(i), valMapper.fromDouble(v));
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // TODO
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO
    }

    @Override public boolean isSequentialAccess() {
        return false;
    }

    @Override public boolean isDense() {
        return false;
    }

    @Override
    public boolean isRandomAccess() {
        return true;
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override public boolean isArrayBased() {
        return false;
    }
}
