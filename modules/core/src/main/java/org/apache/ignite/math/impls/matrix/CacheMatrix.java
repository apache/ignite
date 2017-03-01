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

package org.apache.ignite.math.impls.matrix;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.matrix.*;
import javax.cache.*;
import java.util.*;

/**
 * Matrix based on existing cache and key and value mapping functions.
 */
public class CacheMatrix<K, V> extends AbstractMatrix {
    /**
     *
     */
    public CacheMatrix() {
        // No-op.
    }
    
    /**
     * Creates new matrix over existing cache.
     * 
     * @param rows
     * @param cols
     * @param cache
     * @param keyMapper
     * @param valMapper
     */
    public CacheMatrix(
        int rows,
        int cols,
        IgniteCache<K, V> cache,
        KeyMapper<K> keyMapper,
        ValueMapper<V> valMapper) {
        setStorage(new CacheMatrixStorage<K, V>(rows, cols, cache, keyMapper, valMapper));
    }

    /**
     * @param args
     */
    @SuppressWarnings({"unchecked"})
    public CacheMatrix(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("rows") &&
            args.containsKey("cols") &&
            args.containsKey("keyMapper") &&
            args.containsKey("valMapper") &&
            args.containsKey("cacheName")) {
            int rows = (int)args.get("rows");
            int cols = (int)args.get("cols");
            IgniteCache<K, V> cache = Ignition.localIgnite().getOrCreateCache((String)args.get("cacheName"));
            KeyMapper<K> keyMapper = (KeyMapper<K>)args.get("keyMapper");
            ValueMapper<V> valMapper = (ValueMapper<V>)args.get("valMapper");

            setStorage(new CacheMatrixStorage<K, V>(rows, cols, cache, keyMapper, valMapper));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     *
     * @return
     */
    @SuppressWarnings({"unchecked"})
    private CacheMatrixStorage<K, V> storage() {
        return (CacheMatrixStorage<K, V>)getStorage();
    }

    @Override
    public Matrix copy() {
        CacheMatrixStorage<K, V> sto = storage();

        return new CacheMatrix<K, V>(rowSize(), columnSize(), sto.cache(), sto.keyMapper(), sto.valueMapper());
    }

    @Override
    public Matrix like(int rows, int cols) {
        CacheMatrixStorage<K, V> sto = storage();

        return new CacheMatrix<K, V>(rows, cols, sto.cache(), sto.keyMapper(), sto.valueMapper());
    }

    @Override
    public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix assign(double val) {
        CacheMatrixStorage<K, V> sto = storage();

        // Gets these values assigned to a local vars so that
        // they will be available in the closure.
        String cacheName = sto.cache().getName();
        int partsCnt = partitions(cacheName);
        KeyMapper<K> keyMapper = sto.keyMapper();
        V newVal = sto.valueMapper().fromDouble(val);

        broadcastForCache(cacheName, () -> {
            IgniteCache<K, V> cache = Ignition.localIgnite().getOrCreateCache(cacheName);

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++)
                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part))) {
                    K k = entry.getKey();

                    if (keyMapper.isValid(k))
                        // Actual assignment.
                        cache.put(k, newVal);
                }
        });

        return this;
    }
}
