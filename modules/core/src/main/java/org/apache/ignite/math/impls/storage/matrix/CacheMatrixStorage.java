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

package org.apache.ignite.math.impls.storage.matrix;

import org.apache.ignite.*;
import org.apache.ignite.math.*;
import java.io.*;

/**
 * Matrix storage based on arbitrary cache and key and value mapping functions.
 */
public class CacheMatrixStorage<K, V> implements MatrixStorage {
    private int rows, cols;
    private IgniteCache<K, V> cache;
    private KeyMapper<K> keyMapper;
    private ValueMapper<V> valMapper;

    /**
     *
     */
    public CacheMatrixStorage() {
        // No-op.
    }

    /**
     * 
     * @param rows
     * @param cols
     * @param cache
     * @param keyMapper
     * @param valMapper
     */
    public CacheMatrixStorage(int rows, int cols, IgniteCache<K, V> cache, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        assert rows > 0;
        assert cols > 0;
        assert cache != null;
        assert keyMapper != null;
        assert valMapper != null;

        this.rows = rows;
        this.cols = cols;
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
    public KeyMapper<K> keyMapper() {
        return keyMapper;
    }

    /**
     *
     * @return
     */
    public ValueMapper<V> valueMapper() {
        return valMapper;
    }

    @Override
    public double get(int x, int y) {
        return valMapper.toDouble(cache.get(keyMapper.apply(x, y)));
    }

    @Override
    public void set(int x, int y, double v) {
        cache.put(keyMapper.apply(x, y), valMapper.fromDouble(v));
    }

    @Override
    public int columnSize() {
        return cols;
    }

    @Override
    public int rowSize() {
        return rows;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);
        out.writeUTF(cache.getName());
        out.writeObject(keyMapper);
        out.writeObject(valMapper);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        cache = Ignition.localIgnite().getOrCreateCache(in.readUTF());
        keyMapper = (KeyMapper<K>)in.readObject();
        valMapper = (ValueMapper<V>)in.readObject();
    }

    @Override
    public boolean isSequentialAccess() {
        return false;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public double getLookupCost() {
        return 0;
    }

    @Override
    public boolean isAddConstantTime() {
        return false;
    }

    @Override
    public boolean isArrayBased() {
        return false;
    }
}
