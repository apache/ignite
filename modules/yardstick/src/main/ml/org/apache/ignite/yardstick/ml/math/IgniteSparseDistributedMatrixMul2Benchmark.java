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
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;

/**
 * Ignite benchmark that performs trivially optimized matrix multiplication of {@link SparseDistributedMatrix} that
 * uses delegating to {@link SparseBlockDistributedMatrix}. For unoptimized benchmark refer
 * {@link IgniteSparseDistributedMatrixMulBenchmark}.
 */
@SuppressWarnings("unused")
public class IgniteSparseDistributedMatrixMul2Benchmark extends IgniteAbstractMatrixMulBenchmark {
    /** {@inheritDoc} */
    @Override Matrix newMatrix(int rowSize, int colSize) {
        return new SparseDistributedMatrix(rowSize, colSize,
            StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
    }

    /** {@inheritDoc} */
    @Override public Matrix times(Matrix m1, Matrix m2) {
        return new OptimisedTimes(m1, m2).times();
    }

    /** */
    private static class OptimisedTimes {
        /** */
        private final Matrix m1;

        /** */
        private final Matrix m2;

        /** */
        OptimisedTimes(Matrix m1, Matrix m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        /** */
        Matrix times() {
            Matrix m1Block = new SparseBlockDistributedMatrix(m1.rowSize(), m1.columnSize()).assign(m1);
            Matrix m2Block = new SparseBlockDistributedMatrix(m2.rowSize(), m2.columnSize()).assign(m2);

            Matrix m3Block = m1Block.times(m2Block);

            Matrix res = new SparseDistributedMatrix(m3Block.rowSize(), m3Block.columnSize(),
                StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE).assign(m3Block);

            m1Block.destroy();
            m2Block.destroy();
            m3Block.destroy();

            return res;
        }
    }
}
