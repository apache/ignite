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

package org.apache.ignite.ml.math.impls.storage.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.MatrixKeyMapper;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.ValueMapper;

/**
 * Matrix storage based on arbitrary cache and key and value mapping functions.
 */
public class CacheMatrixStorage<K, V> implements MatrixStorage {
    /** */
    private int rows;
    /** */
    private int cols;
    /** */
    private IgniteCache<K, V> cache;
    /** */
    private MatrixKeyMapper<K> keyMapper;
    /** */
    private ValueMapper<V> valMapper;

    /**
     *
     */
    public CacheMatrixStorage() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in matrix.
     * @param cols Amount of columns in matrix.
     * @param cache Ignite cache.
     * @param keyMapper {@link MatrixKeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain value for given cache key.
     */
    public CacheMatrixStorage(int rows, int cols, IgniteCache<K, V> cache, MatrixKeyMapper<K> keyMapper,
        ValueMapper<V> valMapper) {
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
     * @return Ignite cache.
     */
    public IgniteCache<K, V> cache() {
        return cache;
    }

    /**
     * @return Key mapper.
     */
    public MatrixKeyMapper<K> keyMapper() {
        return keyMapper;
    }

    /**
     * @return Value mapper.
     */
    public ValueMapper<V> valueMapper() {
        return valMapper;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return valMapper.toDouble(cache.get(keyMapper.apply(x, y)));
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        cache.put(keyMapper.apply(x, y), valMapper.fromDouble(v));
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);
        out.writeUTF(cache.getName());
        out.writeObject(keyMapper);
        out.writeObject(valMapper);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        cache = Ignition.localIgnite().getOrCreateCache(in.readUTF());
        keyMapper = (MatrixKeyMapper<K>)in.readObject();
        valMapper = (ValueMapper<V>)in.readObject();
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

        res = res * 37 + rows;
        res = res * 37 + cols;
        res = res * 37 + cache.hashCode();
        res = res * 37 + keyMapper.hashCode();
        res = res * 37 + valMapper.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CacheMatrixStorage that = (CacheMatrixStorage)o;

        return (cache != null ? cache.equals(that.cache) : that.cache == null) &&
            (keyMapper != null ? keyMapper.equals(that.keyMapper) : that.keyMapper == null) &&
            (valMapper != null ? valMapper.equals(that.valMapper) : that.valMapper == null);
    }
}
