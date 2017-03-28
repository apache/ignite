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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.MatrixKeyMapper;
import org.apache.ignite.math.ValueMapper;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.functions.IgniteFunction;
import org.apache.ignite.math.impls.*;
import org.apache.ignite.math.impls.storage.matrix.CacheMatrixStorage;
import org.apache.ignite.math.functions.IgniteDoubleFunction;

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
        MatrixKeyMapper<K> keyMapper,
        ValueMapper<V> valMapper) {
        assert rows > 0;
        assert cols > 0;
        assert cache != null;
        assert keyMapper != null;
        assert valMapper != null;
        
        setStorage(new CacheMatrixStorage<K, V>(rows, cols, cache, keyMapper, valMapper));
    }

    /**
     *
     * @return
     */
    @SuppressWarnings({"unchecked"})
    private CacheMatrixStorage<K, V> storage() {
        return (CacheMatrixStorage<K, V>)getStorage();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param d
     * @return
     */
    @Override public Matrix divide(double d) {
        return mapOverValues((Double v) -> v / d);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x
     * @return
     */
    @Override public Matrix plus(double x) {
        return mapOverValues((Double v) -> v + x);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x
     * @return
     */
    @Override public Matrix times(double x) {
        return mapOverValues((Double v) -> v * x);
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double val) {
        return mapOverValues((Double v) -> val);
    }

    /** {@inheritDoc} */
    @Override public Matrix map(IgniteDoubleFunction<Double> fun) {
        return mapOverValues(fun::apply);
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        CacheMatrixStorage<K, V> sto = storage();

        return CacheUtils.sum(sto.cache().getName(), sto.keyMapper(), sto.valueMapper());
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        CacheMatrixStorage<K, V> sto = storage();

        return CacheUtils.max(sto.cache().getName(), sto.keyMapper(), sto.valueMapper());
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        CacheMatrixStorage<K, V> sto = storage();

        return CacheUtils.min(sto.cache().getName(), sto.keyMapper(), sto.valueMapper());
    }

    /**
     * 
     * @param mapper
     * @return
     */
    private Matrix mapOverValues(IgniteFunction<Double, Double> mapper) {
        CacheMatrixStorage<K, V> sto = storage();

        // Gets these values assigned to a local vars so that
        // they will be available in the closure.
        MatrixKeyMapper<K> keyMapper = sto.keyMapper();
        ValueMapper<V> valMapper = sto.valueMapper();

        CacheUtils.foreach(sto.cache().getName(), (CacheUtils.CacheEntry<K, V> ce) -> {
            K k = ce.entry().getKey();

            if (keyMapper.isValid(k))
                // Actual assignment.
                ce.cache().put(k, valMapper.fromDouble(mapper.apply(valMapper.toDouble(ce.entry().getValue()))));
        });

        return this;
    }
}
