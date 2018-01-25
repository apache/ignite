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

package org.apache.ignite.examples.ml.dlc.transformer;

import com.github.fommil.netlib.BLAS;
import java.util.Arrays;
import org.apache.ignite.ml.dlc.DLC;
import org.apache.ignite.ml.dlc.dataset.DLCWrapper;

/**
 * Algorithm-specific dataset which provides API allows to make gradient descent steps and maintain iteration number.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class AlgorithmSpecificDataset<K, V> extends DLCWrapper<K, V, AlgorithmSpecificReplicatedData, AlgorithmSpecificRecoverableData> {
    /** BlAS instance. */
    private static final BLAS blas = BLAS.getInstance();

    /**
     * Constructs a new instance of Distributed Learning Context wrapper
     *
     * @param delegate delegate which actually performs base functions
     */
    public AlgorithmSpecificDataset(
        DLC<K, V, AlgorithmSpecificReplicatedData, AlgorithmSpecificRecoverableData> delegate) {
        super(delegate);
    }

    /**
     * Make gradient step.
     *
     * @param x point to calculate gradient
     * @return gradient
     */
    public double[] makeGradientStep(double[] x) {
        return compute((part, partIdx) -> {
            AlgorithmSpecificRecoverableData recoverableData = part.getRecoverableData();
            AlgorithmSpecificReplicatedData replicatedData = part.getReplicatedData();

            int iteration = replicatedData.getIteration();

            double[] a = recoverableData.getA();
            double[] b = recoverableData.getB();
            int rows = recoverableData.getRows();
            int cols = recoverableData.getCols();
            double[] bCp = Arrays.copyOf(b, b.length);


            blas.dgemv("N", rows, cols, 1.0, a, Math.max(0, rows), x, 1, -1.0, bCp, 1);

            double[] gradient = new double[cols];
            blas.dgemv("T", rows, cols, 1.0, a, Math.max(0, rows), bCp, 1, 0, gradient, 1);

            replicatedData.setIteration(iteration + 1);

            System.err.println("A = " + Arrays.toString(a) + ", B = " + Arrays.toString(b) + ", rows = " + rows + ", cols = " + cols + ", iter = " + iteration + ", g = " + Arrays.toString(gradient));


            return gradient;
        }, this::reduce);
    }

    /**
     * Calculates sum of two vectors.
     *
     * @param a first vector
     * @param b second vector
     * @return sum of two vectors
     */
    private double[] reduce(double[] a, double[] b) {
        if (a == null)
            return b;

        if (b == null)
            return a;

        blas.daxpy(a.length, 1.0, a, 1, b, 1);

        return b;
    }
}
