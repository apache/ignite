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
    /**
     * Tests that MLP with 2 layer, 1 neuron in each layer and weight equal to 1 is equivalent to sigmoid function.
     */
    @Test
    public void testSimpleMLPPrediction() {
        MLPArchitecture conf = new MLPArchitecture(1).withAddedLayer(1, false, Activators.SIGMOID);

        MLP mlp = new MLP(conf, new MLPConstInitializer(1));

        int input = 2;

        Matrix predict = mlp.apply(new DenseLocalOnHeapMatrix(new double[][] {{input}}));

        Assert.assertEquals(predict, new DenseLocalOnHeapMatrix(new double[][] {{Activators.SIGMOID.apply(input)}}));
    }

    /**
     * Test that MLP with parameters that should produce function close to 'XOR' is close to 'XOR'.
     */
    @Test
    public void testXOR() {
        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(2, true, Activators.SIGMOID).
            withAddedLayer(1, true, Activators.SIGMOID);

        MLP mlp = new MLP(conf, new MLPConstInitializer(1, 2));

        mlp.setWeights(1, new DenseLocalOnHeapMatrix(new double[][] {{20.0, 20.0}, {-20.0, -20.0}}));
        mlp.setBiases(1, new DenseLocalOnHeapVector(new double[] {-10.0, 30.0}));

        mlp.setWeights(2, new DenseLocalOnHeapMatrix(new double[][] {{20.0, 20.0}}));
        mlp.setBiases(2, new DenseLocalOnHeapVector(new double[] {-30.0}));

        Matrix input = new DenseLocalOnHeapMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}}).transpose();

        Matrix predict = mlp.apply(input);
        Vector truth = new DenseLocalOnHeapVector(new double[] {0.0, 1.0, 1.0, 0.0});

        TestUtils.checkIsInEpsilonNeighbourhood(predict.getRow(0), truth, 1E-4);
    }

    /**
     * Test that two layer MLP is equivalent to it's subparts stacked on each other.
     */
    @Test
    public void testStackedMLP() {
        int firstLayerNeuronsCnt = 3;
        int secondLayerNeuronsCnt = 2;
        MLPConstInitializer initer = new MLPConstInitializer(1, 2);

        MLPArchitecture conf = new MLPArchitecture(4).
            withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID).
            withAddedLayer(secondLayerNeuronsCnt, false, Activators.SIGMOID);

        MLP mlp = new MLP(conf, initer);

        MLPArchitecture mlpLayer1Conf = new MLPArchitecture(4).
            withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID);
        MLPArchitecture mlpLayer2Conf = new MLPArchitecture(firstLayerNeuronsCnt).
            withAddedLayer(secondLayerNeuronsCnt, false, Activators.SIGMOID);

        MLP mlp1 = new MLP(mlpLayer1Conf, initer);
        MLP mlp2 = new MLP(mlpLayer2Conf, initer);

        MLP stackedMLP = mlp1.add(mlp2);

        Matrix predict = mlp.apply(new DenseLocalOnHeapMatrix(new double[][] {{1, 2, 3, 4}}).transpose());
        Matrix stackedPredict = stackedMLP.apply(new DenseLocalOnHeapMatrix(new double[][] {{1, 2, 3, 4}}).transpose());

        Assert.assertEquals(predict, stackedPredict);
    }

    @Test
    public void paramsCountTest() {
        int inputSize = 10;
        int layerWithBiasNeuronsCnt = 13;
        int layerWithoutBiasNeuronsCnt = 17;

        MLPArchitecture conf = new MLPArchitecture(inputSize).
            withAddedLayer(layerWithBiasNeuronsCnt, true, Activators.SIGMOID).
            withAddedLayer(layerWithoutBiasNeuronsCnt, false, Activators.SIGMOID);

        Assert.assertEquals(layerWithBiasNeuronsCnt * inputSize + layerWithBiasNeuronsCnt + (layerWithoutBiasNeuronsCnt * layerWithBiasNeuronsCnt),
            conf.parametersCount());
    }

    @Test
    public void setParamsTest() {
        int inputSize = 3;
        int firstLayerNeuronsCnt = 2;
        int secondLayerNeurons = 1;

        DenseLocalOnHeapVector paramsVector = new DenseLocalOnHeapVector(new double[] {
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, // first layer weight matrix
            7.0, 8.0, // second layer weight matrix
            9.0 // second layer biases.
        });

        DenseLocalOnHeapMatrix firstLayerWeights = new DenseLocalOnHeapMatrix(new double[][] {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}});
        DenseLocalOnHeapMatrix secondLayerWeights = new DenseLocalOnHeapMatrix(new double[][] {{7.0, 8.0}});
        DenseLocalOnHeapVector secondLayerBiases = new DenseLocalOnHeapVector(new double[] {9.0});

        MLPArchitecture conf = new MLPArchitecture(inputSize).
            withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID).
            withAddedLayer(secondLayerNeurons, true, Activators.SIGMOID);

        MLP mlp = new MLP(conf, new MLPConstInitializer(100, 200));

        Assert.assertEquals(paramsVector, mlp.setParameters(paramsVector).parameters());

        Assert.assertEquals(mlp.weights(1), firstLayerWeights);
        Assert.assertEquals(mlp.weights(2), secondLayerWeights);
        Assert.assertEquals(mlp.biases(2), secondLayerBiases);
    }
}
