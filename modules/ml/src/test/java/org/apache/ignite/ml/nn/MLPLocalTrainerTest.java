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
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.trainers.local.MLPLocalBatchTrainer;
import org.apache.ignite.ml.nn.updaters.NesterovUpdater;
import org.apache.ignite.ml.nn.updaters.ParameterUpdater;
import org.apache.ignite.ml.nn.updaters.RPropUpdater;
import org.apache.ignite.ml.nn.updaters.SimpleGDUpdater;
import org.apache.ignite.ml.nn.updaters.UpdaterParams;
import org.junit.Test;

/**
 * Tests for {@link MLPLocalBatchTrainer}.
 */
public class MLPLocalTrainerTest {
    /**
     * Test 'XOR' operation training with {@link SimpleGDUpdater} updater.
     */
    @Test
    public void testXORSimpleGD() {
        xorTest(() -> new SimpleGDUpdater(0.3));
    }

    /**
     * Test 'XOR' operation training with {@link RPropUpdater}.
     */
    @Test
    public void testXORRProp() {
        xorTest(RPropUpdater::new);
    }

    /**
     * Test 'XOR' operation training with {@link NesterovUpdater}.
     */
    @Test
    public void testXORNesterov() {
        xorTest(() -> new NesterovUpdater(0.1, 0.7));
    }

    /**
     * Common method for testing 'XOR' with various updaters.
     * @param updaterSupplier Updater supplier.
     * @param <P> Updater parameters type.
     */
    private <P extends UpdaterParams<? super MultilayerPerceptron>> void xorTest(IgniteSupplier<ParameterUpdater<? super MultilayerPerceptron, P>> updaterSupplier) {
        Matrix xorInputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}},
            StorageConstants.ROW_STORAGE_MODE).transpose();

        Matrix xorOutputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0}, {1.0}, {1.0}, {0.0}},
            StorageConstants.ROW_STORAGE_MODE).transpose();

        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(10, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        SimpleMLPLocalBatchTrainerInput trainerInput = new SimpleMLPLocalBatchTrainerInput(conf,
            new Random(1234L), xorInputs, xorOutputs, 4);

        MultilayerPerceptron mlp = new MLPLocalBatchTrainer<>(LossFunctions.MSE,
            updaterSupplier,
            0.0001,
            16000).train(trainerInput);

        Matrix predict = mlp.apply(xorInputs);

        Tracer.showAscii(predict);

        X.println(xorOutputs.getRow(0).minus(predict.getRow(0)).kNorm(2) + "");

        TestUtils.checkIsInEpsilonNeighbourhood(xorOutputs.getRow(0), predict.getRow(0), 1E-1);
    }
}
