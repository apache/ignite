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

package org.apache.ignite.ml.nn;

import java.util.Random;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.util.Utils;

/**
 * Class for local batch training of {@link MultilayerPerceptron}.
 *
 * It is constructed from two matrices: one containing inputs of function to approximate and other containing ground truth
 * values of this function for corresponding inputs.
 *
 * We fix batch size given by this input by some constant value.
 */
public class SimpleMLPLocalBatchTrainerInput implements LocalBatchTrainerInput<MultilayerPerceptron> {
    /**
     * Multilayer perceptron to be trained.
     */
    private final MultilayerPerceptron mlp;

    /**
     * Inputs stored as columns.
     */
    private Matrix inputs;

    /**
     * Ground truths stored as columns.
     */
    private final Matrix groundTruth;

    /**
     * Size of batch returned on each step.
     */
    private int batchSize;

    /**
     * Construct instance of this class.
     *
     * @param arch Architecture of multilayer perceptron.
     * @param rnd Random numbers generator.
     * @param inputs Inputs stored as columns.
     * @param groundTruth Ground truth stored as columns.
     * @param batchSize Size of batch returned on each step.
     */
    public SimpleMLPLocalBatchTrainerInput(MLPArchitecture arch, Random rnd, Matrix inputs, Matrix groundTruth, int batchSize) {
        this.mlp = new MultilayerPerceptron(arch, new RandomInitializer(rnd));
        this.inputs = inputs;
        this.groundTruth = groundTruth;
        this.batchSize = batchSize;
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<Matrix, Matrix> getBatch() {
        int inputRowSize = inputs.rowSize();
        int outputRowSize = groundTruth.rowSize();

        Matrix vectors = new DenseLocalOnHeapMatrix(inputRowSize, batchSize);
        Matrix labels = new DenseLocalOnHeapMatrix(outputRowSize, batchSize);

        int[] samples = Utils.selectKDistinct(inputs.columnSize(), batchSize);

        for (int i = 0; i < batchSize; i++) {
            vectors.assignColumn(i, inputs.getCol(samples[i]));
            labels.assignColumn(i, groundTruth.getCol(samples[i]));
        }

        return new IgniteBiTuple<>(vectors, labels);
    }

    /** {@inheritDoc} */
    @Override public MultilayerPerceptron mdl() {
        return mlp;
    }
}
