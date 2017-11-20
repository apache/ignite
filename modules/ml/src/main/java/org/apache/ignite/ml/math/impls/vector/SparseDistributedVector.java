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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.impls.matrix.*;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.storage.vector.SparseDistributedVectorStorage;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Sparse distributed vector implementation based on data grid.
 * <p>
 * Unlike {@link CacheVector} that is based on existing cache, this implementation creates distributed
 * cache internally and doesn't rely on pre-existing cache.</p>
 * <p>
 * You also need to call {@link #destroy()} to remove the underlying cache when you no longer need this
 * vector.</p>
 * <p>
 * <b>Currently fold supports only commutative operations.<b/></p>
 */
public class SparseDistributedVector extends AbstractVector implements StorageConstants {
    /**
     *
     */
    public SparseDistributedVector() {
        // No-op.
    }

    /**
     * @param size    Vector size.
     * @param acsMode Vector elements access mode..
     */
    public SparseDistributedVector(int size, int acsMode) {

        assert size > 0;
        assertAccessMode(acsMode);


        setStorage(new SparseDistributedVectorStorage(size, acsMode));
    }

    public SparseDistributedVector(int size) {
        this(size, StorageConstants.RANDOM_ACCESS_MODE);
    }

    /**
     * @param data
     */
    public SparseDistributedVector(double[] data) {
        setStorage(new SparseDistributedVectorStorage(data.length, StorageConstants.RANDOM_ACCESS_MODE));
        for (int i = 0; i < data.length; i++) {
            double value = data[i];
            if (value != 0.0) storage().set(i, value);
        }
    }


    /** */
    public SparseDistributedVectorStorage storage() {
        return (SparseDistributedVectorStorage) getStorage();
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param d Value to divide to.
     */
    @Override public Vector divide(double d) {
        return mapOverValues(v -> v / d);
    }

    @Override
    public Vector like(int size) {
        return new SparseDistributedVector(size, storage().accessMode());
    }

    @Override
    public Matrix likeMatrix(int rows, int cols) {
        return new SparseDistributedMatrix(rows, cols);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x Value to add.
     */
    @Override public Vector plus(double x) {
        return mapOverValues(v -> v + x);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x Value to multiply.
     */
    @Override public Vector times(double x) {
        return mapOverValues(v -> v * x);
    }


    /** {@inheritDoc} */
    @Override public Vector assign(double val) {
        return mapOverValues(v -> val);
    }

    /** {@inheritDoc} */
    @Override public Vector map(IgniteDoubleFunction<Double> fun) {
        return mapOverValues(fun);
    }

    /**
     * @param mapper Mapping function.
     * @return Vector with mapped values.
     */
    private Vector mapOverValues(IgniteDoubleFunction<Double> mapper) {
        CacheUtils.sparseMapForVector(getUUID(), mapper, storage().cacheName());

        return this;
    }

    /** */
    public UUID getUUID() {
        return ((SparseDistributedVectorStorage) getStorage()).getUUID();
    }
}
