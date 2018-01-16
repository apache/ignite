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
import org.apache.ignite.ml.dlearn.part.DatasetDLeanPartition;

/** */
public class DatasetMathUtils {
    /** */
    private static final BLAS blas = BLAS.getInstance();

    /** */
    public static double[] mean(DLearnContext<? extends DatasetDLeanPartition> learningCtx, int[] cols) {
        ValueWithCount<double[]> res = learningCtx.compute((part, partIdx) -> {
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
    public static double mean(DLearnContext<? extends DatasetDLeanPartition> learningCtx, int col) {
        return mean(learningCtx, new int[]{col})[0];
    }

    /** */
    public static double[] std(DLearnContext<? extends DatasetDLeanPartition> learningCtx, int[] cols) {
        double[] mean = mean(learningCtx, cols);
        ValueWithCount<double[]> res = learningCtx.compute(part -> {
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

    public static double std(DLearnContext<? extends DatasetDLeanPartition> learningCtx, int col) {
        return std(learningCtx, new int[]{col})[0];
    }

    /** */
    private static double[] sum(double[] a, double[] b) {
        blas.daxpy(a.length, 1.0, a, 1, b, 1);
        return b;
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
