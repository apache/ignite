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

package org.apache.ignite.ml.math.impls.vector;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.ValueMapper;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorKeyMapper;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.CacheUtils;
import org.apache.ignite.ml.math.impls.storage.vector.CacheVectorStorage;

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
     * @param size Vector size.
     * @param cache Ignite cache.
     * @param keyFunc {@link VectorKeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain value for given cache key.
     */
    public CacheVector(
        int size,
        IgniteCache<K, V> cache,
        VectorKeyMapper<K> keyFunc,
        ValueMapper<V> valMapper) {
        setStorage(new CacheVectorStorage<>(size, cache, keyFunc, valMapper));
    }

    /**
     * @param mapper Mapping function.
     */
    private Vector mapOverCache(IgniteFunction<Double, Double> mapper) {
        CacheVectorStorage<K, V> sto = storage();

        CacheUtils.map(sto.cache().getName(), sto.keyMapper(), sto.valueMapper(), mapper);

        return this;
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        CacheVectorStorage<K, V> sto = storage();

        return CacheUtils.min(sto.cache().getName(), sto.keyMapper(), sto.valueMapper());
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        CacheVectorStorage<K, V> sto = storage();

        return CacheUtils.max(sto.cache().getName(), sto.keyMapper(), sto.valueMapper());
    }

    /** {@inheritDoc} */
    @Override public Vector map(IgniteDoubleFunction<Double> fun) {
        return mapOverCache(fun::apply);
    }

    /** {@inheritDoc} */
    @Override public Vector map(IgniteBiFunction<Double, Double, Double> fun, double y) {
        // TODO: provide cache-optimized implementation.
        return super.map(fun, y); // TODO
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        CacheVectorStorage<K, V> sto = storage();

        return CacheUtils.sum(sto.cache().getName(), sto.keyMapper(), sto.valueMapper());
    }

    /** {@inheritDoc} */
    @Override public Vector assign(double val) {
        return mapOverCache((Double d) -> val);
    }

    /** {@inheritDoc} */
    @Override public Vector plus(double x) {
        return mapOverCache((Double d) -> d + x);
    }

    /** {@inheritDoc} */
    @Override public Vector divide(double x) {
        return mapOverCache((Double d) -> d / x);
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        return mapOverCache((Double d) -> d * x);
    }

    /**
     *
     *
     */
    @SuppressWarnings({"unchecked"})
    private CacheVectorStorage<K, V> storage() {
        return (CacheVectorStorage<K, V>)getStorage();
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        throw new UnsupportedOperationException();
    }
}
