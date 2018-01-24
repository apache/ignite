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

package org.apache.ignite.examples.ml.nn;

import java.util.Random;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.trainers.local.LocalBatchTrainerInput;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.nn.trainers.local.MLPLocalBatchTrainer;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.apache.ignite.ml.util.Utils;

/**
 * Example of using local {@link MultilayerPerceptron}.
 */
public class MLPLocalTrainerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        // IMPL NOTE based on MLPLocalTrainerTest#testXORRProp
        System.out.println(">>> Local multilayer perceptron example started.");

        Matrix xorInputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}},
            StorageConstants.ROW_STORAGE_MODE).transpose();

        System.out.println("\n>>> Input data:");

        Tracer.showAscii(xorInputs);

        Matrix xorOutputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0}, {1.0}, {1.0}, {0.0}},
            StorageConstants.ROW_STORAGE_MODE).transpose();

        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(10, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        SimpleMLPLocalBatchTrainerInput trainerInput = new SimpleMLPLocalBatchTrainerInput(conf,
            new Random(1234L), xorInputs, xorOutputs, 4);

        System.out.println("\n>>> Perform training.");

        MultilayerPerceptron mlp = new MLPLocalBatchTrainer<>(LossFunctions.MSE,
            RPropUpdateCalculator::new,
            0.0001,
            16000).train(trainerInput);

        System.out.println("\n>>> Apply model.");

        Matrix predict = mlp.apply(xorInputs);

        System.out.println("\n>>> Predicted data:");

        Tracer.showAscii(predict);

        System.out.println("\n>>> Reference expected data:");

        Tracer.showAscii(xorOutputs);

        System.out.println("\n>>> Difference estimate: " + xorOutputs.getRow(0).minus(predict.getRow(0)).kNorm(2));

        System.out.println("\n>>> Local multilayer perceptron example completed.");
    }

    /**
     * Class for local batch training of {@link MultilayerPerceptron}.
     *
     * It is constructed from two matrices: one containing inputs of function to approximate and other containing ground truth
     * values of this function for corresponding inputs.
     *
     * We fix batch size given by this input by some constant value.
     */
    private static class SimpleMLPLocalBatchTrainerInput implements LocalBatchTrainerInput<MultilayerPerceptron> {
        /**
         * Multilayer perceptron to be trained.
         */
        private final MultilayerPerceptron mlp;

        /**
         * Inputs stored as columns.
         */
        private final Matrix inputs;

        /**
         * Ground truths stored as columns.
         */
        private final Matrix groundTruth;

        /**
         * Size of batch returned on each step.
         */
        private final int batchSize;

        /**
         * Construct instance of this class.
         *
         * @param arch Architecture of multilayer perceptron.
         * @param rnd Random numbers generator.
         * @param inputs Inputs stored as columns.
         * @param groundTruth Ground truth stored as columns.
         * @param batchSize Size of batch returned on each step.
         */
        SimpleMLPLocalBatchTrainerInput(MLPArchitecture arch, Random rnd, Matrix inputs, Matrix groundTruth, int batchSize) {
            this.mlp = new MultilayerPerceptron(arch, new RandomInitializer(rnd));
            this.inputs = inputs;
            this.groundTruth = groundTruth;
            this.batchSize = batchSize;
        }

        /** {@inheritDoc} */
        @Override public IgniteSupplier<IgniteBiTuple<Matrix, Matrix>> batchSupplier() {
            return () -> {
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
            };
        }

        /** {@inheritDoc} */
        @Override public MultilayerPerceptron mdl() {
            return mlp;
        }
    }
}
