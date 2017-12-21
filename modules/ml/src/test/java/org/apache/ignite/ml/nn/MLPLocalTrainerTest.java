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
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.trainers.local.MLPLocalBatchTrainer;
import org.apache.ignite.ml.nn.updaters.NesterovUpdater;
import org.junit.Test;

/**
 * Tests for {@link MLPLocalBatchTrainer}.
 */
public class MLPLocalTrainerTest {
    /**
     * Test 'XOR' operation training.
     */
    @Test
    public void testXOR() {
        Matrix xorInputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}}, StorageConstants.ROW_STORAGE_MODE).transpose();
        Matrix xorOutputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0}, {1.0}, {1.0}, {0.0}}, StorageConstants.ROW_STORAGE_MODE).transpose();

        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(5, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        SimpleMLPLocalBatchTrainerInput trainerInput = new SimpleMLPLocalBatchTrainerInput(conf, new Random(1234L), xorInputs, xorOutputs, 4);

        MLP mdl = MLPLocalBatchTrainer.getDefault().train(trainerInput);

        Matrix predict = mdl.apply(xorInputs);

        TestUtils.checkIsInEpsilonNeighbourhood(xorOutputs.getRow(0), predict.getRow(0), 1E-2);
    }

    /**
     * Test 'XOR' operation training.
     */
    @Test
    public void testXORNesterov() {
        Matrix xorInputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}}, StorageConstants.ROW_STORAGE_MODE).transpose();
        Matrix xorOutputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0}, {1.0}, {1.0}, {0.0}}, StorageConstants.ROW_STORAGE_MODE).transpose();

        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(5, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        SimpleMLPLocalBatchTrainerInput trainerInput = new SimpleMLPLocalBatchTrainerInput(conf, new Random(12345L), xorInputs, xorOutputs, 4);

        MLP mdl = new MLPLocalBatchTrainer(Losses.MSE,
            () -> new NesterovUpdater(0.9),
            0.8,
            0.0001,
            16000).train(trainerInput);

        Matrix predict = mdl.apply(xorInputs);

        TestUtils.checkIsInEpsilonNeighbourhood(xorOutputs.getRow(0), predict.getRow(0), 1E-1);
    }
}
