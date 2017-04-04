// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

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

import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.*;
import org.apache.ignite.math.impls.storage.matrix.*;

/**
 * Sparse distributed matrix implementation based on data grid.
 *
 * Unlike {@link CacheMatrix} that is based on existing cache, this implementation creates distributed
 * cache internally and doesn't rely on pre-existing cache.
 *
 * To get an existing matrix of this type use {@link #locateMatrix(IgniteUuid)} and {@link #guid()} methods.
 * Note that when you create this matrix using its constructors it will create a new independent cache.
 *
 * You also need to call {@link #destroy()} to remove the underlying cache when you no longer need this
 * matrix.
 */
public class SparseDistributedMatrix extends AbstractMatrix implements StorageConstants {
    /**
     *
     */
    public SparseDistributedMatrix() {
        // No-op.
    }

    /**
     *
     * @param rows
     * @param cols
     * @param stoMode
     * @param acsMode
     */
    public SparseDistributedMatrix(int rows, int cols, int stoMode, int acsMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        setStorage(new SparseDistributedMatrixStorage(rows, cols, stoMode, acsMode));
    }

    /**
     *
     * @param guid
     * @return
     */
    public static SparseDistributedMatrix locateMatrix(IgniteUuid guid) {
        return null; // TODO
    }

    /**
     *
     * @return
     */
    private SparseDistributedMatrixStorage storage() {
        return (SparseDistributedMatrixStorage)getStorage();
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        SparseDistributedMatrixStorage sto = storage();

        return CacheUtils.sparseSum(sto.cache().getName());
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        SparseDistributedMatrixStorage sto = storage();

        return CacheUtils.sparseMax(sto.cache().getName());
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        SparseDistributedMatrixStorage sto = storage();

        return CacheUtils.sparseMin(sto.cache().getName());
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
}
