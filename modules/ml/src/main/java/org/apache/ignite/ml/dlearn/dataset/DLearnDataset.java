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

package org.apache.ignite.ml.dlearn.dataset;

import com.github.fommil.netlib.BLAS;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.dataset.part.DLeanDatasetPartition;

/** */
public class DLearnDataset<P extends DLeanDatasetPartition> extends AbstractDLearnContextWrapper<P> {
    /** */
    private static final BLAS blas = BLAS.getInstance();

    /** */
    public DLearnDataset(DLearnContext<P> delegate) {
        super(delegate);
    }

    /** */
    public double[] mean(int[] cols) {
        ValueWithCount<double[]> res = delegate.compute((part, partIdx) -> {
            double[] features = part.getFeatures();
            int m = part.getRows();
            double[] y = new double[cols.length];
            for (int i = 0; i < cols.length; i++)
                for (int j = cols[i] * m; j < (cols[i] + 1) * m; j++)
                    y[i] += features[j];
            return new ValueWithCount<>(y, m);
        }, (a, b) -> a == null ? b : new ValueWithCount<>(sum(a.val, b.val), a.cnt + b.cnt));
        blas.dscal(res.val.length, 1.0 / res.cnt, res.val, 1);
        return res.val;
    }

    /** */
    public double mean(int col) {
        return mean(new int[]{col})[0];
    }

    /** */
    public double[] std(int[] cols) {
        double[] mean = mean(cols);
        ValueWithCount<double[]> res = delegate.compute(part -> {
            double[] features = part.getFeatures();
            int m = part.getRows();
            double[] y = new double[cols.length];
            for (int i = 0; i < cols.length; i++)
                for (int j = cols[i] * m; j < (cols[i] + 1) * m; j++)
                    y[i] += Math.pow(features[j] - mean[cols[i]], 2);
            return new ValueWithCount<>(y, m);
        }, (a, b) -> a == null ? b : new ValueWithCount<>(sum(a.val, b.val), a.cnt + b.cnt));
        blas.dscal(res.val.length, 1.0 / res.cnt, res.val, 1);
        for (int i = 0; i < res.val.length; i++)
            res.val[i] = Math.sqrt(res.val[i]);
        return res.val;
    }

    /** */
    public double std(int col) {
        return std(new int[]{col})[0];
    }

    /** */
    public double[][] cov(int[] cols) {
        double[] mean = mean(cols);
        ValueWithCount<double[][]> res = delegate.compute(part -> {
            double[] features = part.getFeatures();
            int m = part.getRows();
            double[][] y = new double[cols.length][cols.length];
            for (int i = 0; i < cols.length; i++)
                for (int j = 0; j < cols.length; j++) {
                    int firstCol = cols[i];
                    int secondCol = cols[j];
                    for (int k = 0; k < m; k++) {
                        double firstVal = features[m * firstCol + k];
                        double secondVal = features[m * secondCol + k];
                        y[i][j] += ((firstVal - mean[firstCol]) * (secondVal - mean[secondCol]));
                    }
                }
            return new ValueWithCount<>(y, m);
        }, (a, b) -> a == null ? b : new ValueWithCount<>(sum(a.val, b.val), a.cnt + b.cnt));
        return scale(res.val, 1.0 / res.cnt);
    }

    /** */
    public double[][] corr(int[] cols) {
        double[][] cov = cov(cols);
        double[] std = std(cols);
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

    private static double[][] sum(double[][] a, double[][] b) {
        for (int i = 0; i < a.length; i++)
            for (int j = 0; j < a[i].length; j++)
                a[i][j] += b[i][j];
        return a;
    }

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
        public ValueWithCount(V val, int cnt) {
            this.val = val;
            this.cnt = cnt;
        }
    }
}
