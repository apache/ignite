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

package org.apache.ignite.yardstick.ml.math;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;

/**
 * Ignite benchmark that performs unoptimized matrix multiplication of {@link SparseDistributedMatrix}.
 * For optimized benchmark refer {@link IgniteSparseBlockDistributedMatrixMulBenchmark}.
 */
@SuppressWarnings("unused")
public class IgniteSparseDistributedMatrixMulBenchmark extends IgniteAbstractMatrixMulBenchmark {
    /** This benchmark is unacceptably slow without scaling down, see IGNITE-7097. */
    private static final int SCALE_DOWN = 2;

    /** {@inheritDoc} */
    @Override Matrix newMatrix(int rowSize, int colSize) {
        return new SparseDistributedMatrix(rowSize >> SCALE_DOWN, colSize >> SCALE_DOWN,
            StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
    }

    /** {@inheritDoc} */
    @Override Matrix createAndFill(double[][] data, double scale) {
        Matrix res = newMatrix(data.length, data[0].length);

        for (int row = 0; row < res.rowSize(); row++)
            for (int col = 0; col < res.columnSize(); col++)
                res.set(row, col, row == col ? data[row][col] * scale : data[row][col]);

        return res;
    }
}
