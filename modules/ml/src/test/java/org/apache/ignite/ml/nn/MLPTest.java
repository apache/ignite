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

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Multilayer perceptron.
 */
public class MLPTest {
    @Test
    public void simplestMLPTest() {
        MLPArchitecture conf = new MLPArchitecture(1).withAddedLayer(1, false, Activators.SIGMOID);

        MLP mlp = new MLP(conf, new MLPConstInitializer(2, 1));

        int input = 1;

        Matrix predict = mlp.predict(new DenseLocalOnHeapMatrix(new double[][] {{input}}));

        Assert.assertEquals(predict, new DenseLocalOnHeapMatrix(new double[][] {{Activators.SIGMOID.apply(input)}}));
    }

    @Test
    public void xorTest() {
        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(2, true, Activators.SIGMOID).
            withAddedLayer(1, true, Activators.SIGMOID);

        MLP mlp = new MLP(conf, new MLPConstInitializer(2, 1));

        mlp.setWeights(1, new DenseLocalOnHeapMatrix(new double[][] {{20.0, 20.0}, {-20.0, -20.0}}));
        mlp.setBiases(1, new DenseLocalOnHeapVector(new double[] {-10.0, 30.0}));

        mlp.setWeights(2, new DenseLocalOnHeapMatrix(new double[][] {{20.0, 20.0}}));
        mlp.setBiases(2, new DenseLocalOnHeapVector(new double[] {-30.0}));

        Matrix input = new DenseLocalOnHeapMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}}).transpose();

        Matrix predict = mlp.predict(input);
        Vector truth = new DenseLocalOnHeapVector(new double[] {0.0, 1.0, 1.0, 0.0});

        TestUtils.checkIsInEpsilonNeighbourhood(predict.getRow(0), truth, 1E-4);
    }

    @Test
    public void twoLayerMLPTest() {
        MLPArchitecture conf = new MLPArchitecture(4).
            withAddedLayer(3, false, Activators.SIGMOID).
            withAddedLayer(2, false, Activators.SIGMOID);

        MLP mlp = new MLP(conf, new MLPConstInitializer(2, 1));

        Matrix predict = mlp.predict(new DenseLocalOnHeapMatrix(new double[][] {{1, 2, 3, 4}}).transpose());

        Tracer.showAscii(predict);
    }

    @Test
    public void stackedMLPTest() {
        int firstLayerNeuronsCnt = 3;
        int secondLayerNeuronsCnt = 2;
        MLPConstInitializer initer = new MLPConstInitializer(2, 1);

        MLPArchitecture conf = new MLPArchitecture(4).
            withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID).
            withAddedLayer(secondLayerNeuronsCnt, false, Activators.SIGMOID);

        MLP mlp = new MLP(conf, initer);

        MLPArchitecture mlpLayer1Conf = new MLPArchitecture(4).withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID);
        MLPArchitecture mlpLayer2Conf = new MLPArchitecture(firstLayerNeuronsCnt).withAddedLayer(secondLayerNeuronsCnt, false, Activators.SIGMOID);

        MLP mlp1 = new MLP(mlpLayer1Conf, initer);
        MLP mlp2 = new MLP(mlpLayer2Conf, initer);

        MLP stackedMLP = mlp2.add(mlp1);

        Matrix predict = mlp.predict(new DenseLocalOnHeapMatrix(new double[][] {{1, 2, 3, 4}}).transpose());
        Matrix stackedPredict = stackedMLP.predict(new DenseLocalOnHeapMatrix(new double[][] {{1, 2, 3, 4}}).transpose());

        Assert.assertEquals(predict, stackedPredict);
    }
}
