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

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.ml.DataChanger;

/**
 * Ignite benchmark that performs ML Grid operations of matrix multiplication.
 */
abstract class IgniteAbstractMatrixMulBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static final int SIZE = 1 << 7;

    /** */
    private double[][] dataSquare = createAndFill(SIZE, SIZE);

    /** */
    private double[][] dataRect1 = createAndFill(SIZE / 2, SIZE);

    /** */
    private double[][] dataRect2 = createAndFill(SIZE, SIZE / 2);

    /** */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final double scale = DataChanger.next();

        // Create IgniteThread, we may want to work with SparseDistributedMatrix inside IgniteThread
        // because we create ignite cache internally.
        IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
            this.getClass().getSimpleName(), new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                Matrix m1, m2, m3, m4, m5, m6;

                Matrix m7 = times(m1 = createAndFill(dataSquare, scale), m2 = createAndFill(dataSquare, scale));
                Matrix m8 = times(m3 = createAndFill(dataRect1, scale), m4 = createAndFill(dataRect2, scale));
                Matrix m9 = times(m5 = createAndFill(dataRect2, scale), m6 = createAndFill(dataRect1, scale));

                m1.destroy();
                m2.destroy();
                m3.destroy();
                m4.destroy();
                m5.destroy();
                m6.destroy();
                m7.destroy();
                m8.destroy();
                m9.destroy();
            }
        });

        igniteThread.start();

        igniteThread.join();

        return true;
    }

    /**
     * Override in subclasses with specific type Matrix. Note that size of result matrix may be smaller than requested.
     *
     * @param rowSize Requested row size.
     * @param colSize Requested column size.
     * @return Matrix of desired type of size that doesn't exceed requested.
     */
    abstract Matrix newMatrix(int rowSize, int colSize);

    /** Override in subclasses if needed. */
    Matrix times(Matrix m1, Matrix m2) {
        return m1.times(m2);
    }

    /** Override in subclasses if needed to account for smaller matrix size. */
    Matrix createAndFill(double[][] data, double scale) {
        // IMPL NOTE yardstick 0.8.3 fails to discover benchmark if we try to account for smaller size
        //  matrix by using method with lambda parameter here: assign((row, col) -> data[row][col]).
        Matrix res = newMatrix(data.length, data[0].length).assign(data);

        for (int i = 0; i < data.length && i < data[0].length; i++)
            res.set(i, i, data[i][i] * scale);

        return res;
    }

    /** */
    private double[][] createAndFill(int rowSize, int colSize) {
        double[][] data = new double[rowSize][colSize];

        for (int i = 0; i < rowSize; i++)
            for (int j = 0; j < colSize; j++)
                data[i][j] = -0.5d + Math.random();

        return data;
    }
}
