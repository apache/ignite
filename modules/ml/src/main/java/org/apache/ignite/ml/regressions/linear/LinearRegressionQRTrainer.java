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
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.decompositions.QRDSolver;
import org.apache.ignite.ml.math.decompositions.QRDecomposition;
import org.apache.ignite.ml.math.impls.vector.FunctionVector;

/**
 * Linear regression trainer based on least squares loss function and QR decomposition.
 */
public class LinearRegressionQRTrainer implements Trainer<LinearRegressionModel, Matrix> {
    /**
     * {@inheritDoc}
     */
    @Override public LinearRegressionModel train(Matrix data) {
        Vector groundTruth = extractGroundTruth(data);
        Matrix inputs = extractInputs(data);

        QRDecomposition decomposition = new QRDecomposition(inputs);
        QRDSolver solver = new QRDSolver(decomposition.getQ(), decomposition.getR());

        Vector variables = solver.solve(groundTruth);
        Vector weights = variables.viewPart(1, variables.size() - 1);

        double intercept = variables.get(0);

        return new LinearRegressionModel(weights, intercept);
    }

    /**
     * Extracts first column with ground truth from the data set matrix.
     *
     * @param data data to build model
     * @return Ground truth vector
     */
    private Vector extractGroundTruth(Matrix data) {
        return data.getCol(0);
    }

    /**
     * Extracts all inputs from data set matrix and updates matrix so that first column contains value 1.0.
     *
     * @param data data to build model
     * @return Inputs matrix
     */
    private Matrix extractInputs(Matrix data) {
        data = data.copy();

        data.assignColumn(0, new FunctionVector(data.rowSize(), row -> 1.0));

        return data;
    }
}
