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

package org.apache.ignite.ml.dataset.primitive;

import java.io.Serializable;
import com.github.fommil.netlib.BLAS;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.data.SimpleDatasetData;

/**
 * A simple dataset introduces additional methods based on a matrix of features.
 *
 * @param <C> Type of a partition {@code context}.
 */
public class SimpleDataset<C extends Serializable> extends DatasetWrapper<C, SimpleDatasetData> {
    /** BLAS (Basic Linear Algebra Subprograms) instance. */
    private static final BLAS blas = BLAS.getInstance();

    /**
     * Creates a new instance of simple dataset that introduces additional methods based on a matrix of features.
     *
     * @param delegate Delegate that performs {@code compute} actions.
     */
    public SimpleDataset(Dataset<C, SimpleDatasetData> delegate) {
        super(delegate);
    }

    /**
     * Calculates mean value by all columns.
     *
     * @return Mean values.
     */
    public double[] mean() {
        ValueWithCount<double[]> res = delegate.compute((data, partIdx) -> {
            double[] features = data.getFeatures();
            int rows = data.getRows();
            int cols = features.length / rows;

            double[] y = new double[cols];

            for (int col = 0; col < cols; col++)
                for (int j = col * rows; j < (col + 1) * rows; j++)
                    y[col] += features[j];

            return new ValueWithCount<>(y, rows);
        }, (a, b) -> a == null ? b : b == null ? a : new ValueWithCount<>(sum(a.val, b.val), a.cnt + b.cnt));

        if (res != null) {
            blas.dscal(res.val.length, 1.0 / res.cnt, res.val, 1);
            return res.val;
        }

        return null;
    }

    /**
     * Calculates standard deviation by all columns.
     *
     * @return Standard deviations.
     */
    public double[] std() {
        double[] mean = mean();
        ValueWithCount<double[]> res = delegate.compute(data -> {
            double[] features = data.getFeatures();
            int rows = data.getRows();
            int cols = features.length / rows;

            double[] y = new double[cols];

            for (int col = 0; col < cols; col++)
                for (int j = col * rows; j < (col + 1) * rows; j++)
                    y[col] += Math.pow(features[j] - mean[col], 2);

            return new ValueWithCount<>(y, rows);
        }, (a, b) -> a == null ? b : b == null ? a : new ValueWithCount<>(sum(a.val, b.val), a.cnt + b.cnt));

        if (res != null) {
            blas.dscal(res.val.length, 1.0 / res.cnt, res.val, 1);
            for (int i = 0; i < res.val.length; i++)
                res.val[i] = Math.sqrt(res.val[i]);
            return res.val;
        }

        return null;
    }

    /**
     * Calculates covariance matrix by all columns.
     *
     * @return Covariance matrix.
     */
    public double[][] cov() {
        double[] mean = mean();
        ValueWithCount<double[][]> res = delegate.compute(data -> {
            double[] features = data.getFeatures();
            int rows = data.getRows();
            int cols = features.length / rows;

            double[][] y = new double[cols][cols];

            for (int firstCol = 0; firstCol < cols; firstCol++)
                for (int secondCol = 0; secondCol < cols; secondCol++) {

                    for (int k = 0; k < rows; k++) {
                        double firstVal = features[rows * firstCol + k];
                        double secondVal = features[rows * secondCol + k];
                        y[firstCol][secondCol] += ((firstVal - mean[firstCol]) * (secondVal - mean[secondCol]));
                    }
                }

            return new ValueWithCount<>(y, rows);
        }, (a, b) -> a == null ? b : b == null ? a : new ValueWithCount<>(sum(a.val, b.val), a.cnt + b.cnt));

        return res != null ? scale(res.val, 1.0 / res.cnt) : null;
    }

    /**
     * Calculates correlation matrix by all columns.
     *
     * @return Correlation matrix.
     */
    public double[][] corr() {
        double[][] cov = cov();
        double[] std = std();

        for (int i = 0; i < cov.length; i++)
            for (int j = 0; j < cov[0].length; j++)
                cov[i][j] /= (std[i] * std[j]);

        return cov;
    }

    /**
     * Returns the sum of the two specified vectors.  Be aware that it is in-place operation.
     *
     * @param a First vector.
     * @param b Second vector.
     * @return Sum of the two specified vectors.
     */
    private static double[] sum(double[] a, double[] b) {
        for (int i = 0; i < a.length; i++)
            a[i] += b[i];

        return a;
    }

    /**
     * Returns the sum of the two specified matrices. Be aware that it is in-place operation.
     *
     * @param a First matrix.
     * @param b Second matrix.
     * @return Sum of the two specified matrices.
     */
    private static double[][] sum(double[][] a, double[][] b) {
        for (int i = 0; i < a.length; i++)
            for (int j = 0; j < a[i].length; j++)
                a[i][j] += b[i][j];

        return a;
    }

    /**
     * Multiplies all elements of the specified matrix on specified multiplier {@code alpha}. Be aware that it is
     * in-place operation.
     *
     * @param a Matrix to be scaled.
     * @param alpha Multiplier.
     * @return Scaled matrix.
     */
    private static double[][] scale(double[][] a, double alpha) {
        for (int i = 0; i < a.length; i++)
            for (int j = 0; j < a[i].length; j++)
                a[i][j] *= alpha;

        return a;
    }

    /**
     * Util class that keeps values and count of rows this value has been calculated on.
     *
     * @param <V> Type of a value.
     */
    private static class ValueWithCount<V> {
        /** Value. */
        private final V val;

        /** Count of rows the value has been calculated on. */
        private final int cnt;

        /**
         * Constructs a new instance of value with count.
         *
         * @param val Value.
         * @param cnt Count of rows the value has been calculated on.
         */
        ValueWithCount(V val, int cnt) {
            this.val = val;
            this.cnt = cnt;
        }
    }
}
