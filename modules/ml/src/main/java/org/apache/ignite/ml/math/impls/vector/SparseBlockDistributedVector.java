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

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.BlockMatrixStorage;
import org.apache.ignite.ml.math.impls.storage.matrix.BlockVectorStorage;
import org.apache.ignite.ml.math.impls.storage.vector.SparseDistributedVectorStorage;

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
public class SparseBlockDistributedVector extends AbstractVector implements StorageConstants {
    /**
     *
     */
    public SparseBlockDistributedVector() {
        // No-op.
    }

    /**
     * @param size Vector size
     */
    public SparseBlockDistributedVector(int size) {

        assert size > 0;
        setStorage(new BlockVectorStorage(size));
    }


    /**
     *
     * @param data Data to fill storage
     */
    public SparseBlockDistributedVector(double[] data) {
        setStorage(new BlockVectorStorage(data.length));
        for (int i = 0; i < data.length; i++) {
            double val = data[i];
            if(val != 0.0) storage().set(i, val);
        }
    }



    /** */
    public BlockVectorStorage storage() {
        return (BlockVectorStorage)getStorage();
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param d Value to divide to.
     */
    @Override public Vector divide(double d) {
        return mapOverValues(v -> v / d);
    }

    @Override public Vector like(int size) {
        return new SparseBlockDistributedVector(size);
    }

    @Override public Matrix likeMatrix(int rows, int cols) {
        return new SparseBlockDistributedMatrix(rows, cols);
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
    public IgniteUuid getUUID() {
        return ((BlockVectorStorage)getStorage()).getUUID();
    }
}
