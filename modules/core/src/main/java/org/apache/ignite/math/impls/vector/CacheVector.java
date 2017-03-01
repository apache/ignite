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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.vector.CacheVectorStorage;

import java.util.*;
import java.util.function.*;

/**
 * Vector based on existing cache and index and value mapping functions.
 */
public class CacheVector<K, V> extends AbstractVector {
    /**
     *
     */
    public CacheVector() {
        // No-op.
    }

    /**
     * Creates new vector over existing cache.
     *
     * @param size
     * @param cache
     * @param keyFunc
     * @param valMapper
     */
    public CacheVector(
        int size,
        IgniteCache<K, V> cache,
        IntFunction<K> keyFunc,
        ValueMapper<V> valMapper) {
        setStorage(new CacheVectorStorage<K, V>(size, cache, keyFunc, valMapper));
    }

    /**
     * @param args
     */
    @SuppressWarnings({"unchecked"})
    public CacheVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size") &&
            args.containsKey("keyFunc") &&
            args.containsKey("valMapper") &&
            args.containsKey("cacheName")) {
            int size = (int)args.get("size");
            IgniteCache<K, V> cache = Ignition.localIgnite().getOrCreateCache((String)args.get("cacheName"));
            IntFunction<K> keyFunc = (IntFunction<K>)args.get("keyFunc");
            ValueMapper<V> valMapper = (ValueMapper<V>)args.get("valMapper");

            setStorage(new CacheVectorStorage<K, V>(size, cache, keyFunc, valMapper));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     *
     * @return
     */
    @SuppressWarnings({"unchecked"})
    private CacheVectorStorage<K, V> storage() {
        return (CacheVectorStorage<K, V>)getStorage();
    }

    @Override
    public Vector like(int crd) {
        CacheVectorStorage<K, V> sto = storage();

        return new CacheVector<K, V>(size(), sto.cache(), sto.keyFunction(), sto.valueMapper());
    }

    @Override
    public Matrix likeMatrix(int rows, int cols) {
        throw new UnsupportedOperationException();
    }
}
