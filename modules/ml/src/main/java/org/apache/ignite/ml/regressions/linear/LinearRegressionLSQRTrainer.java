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

package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.dataset.part.DLearnLabeledDatasetPartition;

/** */
public class LinearRegressionLSQRTrainer implements Trainer<LinearRegressionModel, DLearnContext<? extends DLearnLabeledDatasetPartition>> {
    /** */
    private static final String A_NAME = "a";

    /** */
    @Override public LinearRegressionModel train(DLearnContext<? extends DLearnLabeledDatasetPartition> learningCtx) {
        preProcessContext(learningCtx);

//        DistributedLSQR<?> lsqr = new DistributedLSQR<>(
//            learningCtx,
//            part -> part.get(A_NAME),
//            LabeledDatasetLearningContextPartition::getLabels
//        );
//
//        LSQRResult res = lsqr.solve(0, 1e-8, 1e-8, 1e8, -1, false, null);
//
//        postProcessContext(learningCtx);
//
//        double[] x = res.getX();
//        double[] weights = Arrays.copyOfRange(x, 1, x.length);
//        double intercept = x[0];
//
//        return new LinearRegressionModel(new DenseLocalOnHeapVector(weights), intercept);
        return null;
    }

    /**
     * Processing of given learning context before training.
     */
    private void preProcessContext(DLearnContext<? extends DLearnLabeledDatasetPartition> learningCtx) {
        learningCtx.compute(part -> {
            double[] features = part.getFeatures();

            int rows = part.getRows();
            double[] a = addInterceptCoefficientColumn(features, rows);

//            part.put(A_NAME, a);
        });
    }

    /**
     * Processing of given learning context after training.
     */
    private void postProcessContext(DLearnContext<? extends DLearnLabeledDatasetPartition> learningCtx) {
//        learningCtx.compute(part -> part.remove(A_NAME));
    }

    /**
     * Adds intercept coefficient (1.0) columns to the matrix.
     *
     * @return matrix with intercept coefficient column
     */
    private double[] addInterceptCoefficientColumn(double[] features, int rows) {
        double[] res = new double[features.length + rows];
        System.arraycopy(features, 0, res, rows, features.length);
        for (int i = 0; i < rows; i++)
            res[i] = 1.0;
        return res;
    }
}
