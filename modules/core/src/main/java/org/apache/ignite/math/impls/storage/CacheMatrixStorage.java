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

package org.apache.ignite.math.impls.storage;

import org.apache.ignite.*;
import org.apache.ignite.math.*;
import java.io.*;

/**
 * Matrix storage based on arbitrary cache and key and value mapping functions.
 */
public class CacheMatrixStorage<K, V> implements MatrixStorage {
    private int rows, cols;
    private IgniteCache<K, V> cache;
    private IntIntToKFunction<K> keyFunc;
    private DoubleMapper<V> valMapper;

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
     * @param keyFunc
     * @param valMapper
     */
    public CacheMatrixStorage(int rows, int cols, IgniteCache<K, V> cache, IntIntToKFunction<K> keyFunc, DoubleMapper<V> valMapper) {
        this.rows = rows;
        this.cols = cols;
        this.cache = cache;
        this.keyFunc = keyFunc;
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
    public IntIntToKFunction<K> keyFunction() {
        return keyFunc;
    }

    /**
     *
     * @return
     */
    public DoubleMapper<V> valueMapper() {
        return valMapper;
    }

    @Override
    public double get(int x, int y) {
        return valMapper.toDouble(cache.get(keyFunc.apply(x, y)));
    }

    @Override
    public void set(int x, int y, double v) {
        cache.put(keyFunc.apply(x, y), valMapper.fromDouble(v));
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
        out.writeObject(keyFunc);
        out.writeObject(valMapper);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        cache = Ignition.localIgnite().getOrCreateCache(in.readUTF());
        keyFunc = (IntIntToKFunction<K>)in.readObject();
        valMapper = (DoubleMapper<V>)in.readObject();
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
