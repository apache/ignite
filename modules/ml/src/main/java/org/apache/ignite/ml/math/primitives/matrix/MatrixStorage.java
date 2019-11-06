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

package org.apache.ignite.ml.math.primitives.matrix;

import java.io.Externalizable;
import org.apache.ignite.ml.math.Blas;
import org.apache.ignite.ml.math.Destroyable;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.StorageOpsMetrics;

/**
 * Data storage support for {@link Matrix}.
 */
public interface MatrixStorage extends Externalizable, StorageOpsMetrics, Destroyable {
    /**
     * @param x Matrix row index.
     * @param y Matrix column index.
     * @return Value corresponding to given row and column.
     */
    public double get(int x, int y);

    /**
     * @param x Matrix row index.
     * @param y Matrix column index.
     * @param v Value to set at given row and column.
     */
    public void set(int x, int y, double v);

    /**
     *
     */
    public int columnSize();

    /**
     *
     */
    public int rowSize();

    /**
     * @return Matrix elements storage mode.
     * @see StorageConstants
     */
    public int storageMode();

    /**
     * @return Matrix access mode.
     * @see StorageConstants
     */
    public int accessMode();

    /**
     * Gets underlying data, if {@link StorageOpsMetrics#isArrayBased()} returns {@code false} this method return
     * copy of data. The data must be adapted for {@link Blas}.
     *
     * @see StorageOpsMetrics#isArrayBased()
     */
    public default double[] data() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public default boolean isNumeric() {
        return true;
    }
}
