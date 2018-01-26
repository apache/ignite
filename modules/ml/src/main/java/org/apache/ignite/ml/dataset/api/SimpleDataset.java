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

package org.apache.ignite.ml.dataset.api;

import com.github.fommil.netlib.BLAS;
import java.io.Serializable;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.api.data.SimpleDatasetData;

public class SimpleDataset<C extends Serializable> extends DatasetWrapper<C, SimpleDatasetData> {
    /** BLAS (Basic Linear Algebra Subprograms) instance. */
    private static final BLAS blas = BLAS.getInstance();

    /**
     *
     * @param delegate
     */
    public SimpleDataset(Dataset<C, SimpleDatasetData> delegate) {
        super(delegate);
    }

    /**
     * Calculates mean value by all columns.
     *
     * @return mean values
     */
    public double[] mean() {
        ValueWithCount<double[]> res = delegate.compute((data, partIdx) -> {
            double[] features = data.getFeatures();
            int rows = data.getRows();
            int cols = data.getCols();

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
     * @return standard deviations
     */
    public double[] std() {
        double[] mean = mean();
        ValueWithCount<double[]> res = delegate.compute(data -> {
            double[] features = data.getFeatures();
            int rows = data.getRows();
            int cols = data.getCols();

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
     * @return covariance matrix
     */
    public double[][] cov() {
        double[] mean = mean();
        ValueWithCount<double[][]> res = delegate.compute(data -> {
            double[] features = data.getFeatures();
            int rows = data.getRows();
            int cols = data.getCols();

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
     * @return correlation matrix
     */
    public double[][] corr() {
        double[][] cov = cov();
        double[] std = std();

        for (int i = 0; i < cov.length; i++)
            for (int j = 0; j < cov[0].length; j++)
                cov[i][j] /= (std[i]*std[j]);

        return cov;
    }

    /** */
    private static double[] sum(double[] a, double[] b) {
        for (int i = 0; i < a.length; i++)
            a[i] += b[i];

        return a;
    }

    /** */
    private static double[][] sum(double[][] a, double[][] b) {
        for (int i = 0; i < a.length; i++)
            for (int j = 0; j < a[i].length; j++)
                a[i][j] += b[i][j];

        return a;
    }

    /** */
    private static double[][] scale(double[][] a, double alpha) {
        for (int i = 0; i < a.length; i++)
            for (int j = 0; j < a[i].length; j++)
                a[i][j] *= alpha;

        return a;
    }

    /** */
    private static class ValueWithCount<V> {
        /** */
        private final V val;

        /** */
        private final int cnt;

        /** */
        ValueWithCount(V val, int cnt) {
            this.val = val;
            this.cnt = cnt;
        }
    }
}
