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
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link MultilayerPerceptron}.
 */
public class MLPTest {
    /**
     * Tests that MLP with 2 layer, 1 neuron in each layer and weight equal to 1 is equivalent to sigmoid function.
     */
    @Test
    public void testSimpleMLPPrediction() {
        MLPArchitecture conf = new MLPArchitecture(1).withAddedLayer(1, false, Activators.SIGMOID);

        MultilayerPerceptron mlp = new MultilayerPerceptron(conf, new MLPConstInitializer(1));

        int input = 2;

        Matrix predict = mlp.apply(new DenseMatrix(new double[][] {{input}}));

        Assert.assertEquals(predict, new DenseMatrix(new double[][] {{Activators.SIGMOID.apply(input)}}));
    }

    /**
     * Test that MLP with parameters that should produce function close to 'XOR' is close to 'XOR' on 'XOR' domain.
     */
    @Test
    public void testXOR() {
        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(2, true, Activators.SIGMOID).
            withAddedLayer(1, true, Activators.SIGMOID);

        MultilayerPerceptron mlp = new MultilayerPerceptron(conf, new MLPConstInitializer(1, 2));

        mlp.setWeights(1, new DenseMatrix(new double[][] {{20.0, 20.0}, {-20.0, -20.0}}));
        mlp.setBiases(1, new DenseVector(new double[] {-10.0, 30.0}));

        mlp.setWeights(2, new DenseMatrix(new double[][] {{20.0, 20.0}}));
        mlp.setBiases(2, new DenseVector(new double[] {-30.0}));

        Matrix input = new DenseMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}});

        Matrix predict = mlp.apply(input);
        Matrix truth = new DenseMatrix(new double[][] {{0.0}, {1.0}, {1.0}, {0.0}});

        TestUtils.checkIsInEpsilonNeighbourhood(predict.getRow(0), truth.getRow(0), 1E-4);
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

        MultilayerPerceptron mlp = new MultilayerPerceptron(conf, initer);

        MLPArchitecture mlpLayer1Conf = new MLPArchitecture(4).
            withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID);
        MLPArchitecture mlpLayer2Conf = new MLPArchitecture(firstLayerNeuronsCnt).
            withAddedLayer(secondLayerNeuronsCnt, false, Activators.SIGMOID);

        MultilayerPerceptron mlp1 = new MultilayerPerceptron(mlpLayer1Conf, initer);
        MultilayerPerceptron mlp2 = new MultilayerPerceptron(mlpLayer2Conf, initer);

        MultilayerPerceptron stackedMLP = mlp1.add(mlp2);

        Matrix predict = mlp.apply(new DenseMatrix(new double[][] {{1}, {2}, {3}, {4}}).transpose());
        Matrix stackedPredict = stackedMLP.apply(new DenseMatrix(new double[][] {{1}, {2}, {3}, {4}}).transpose());

        Assert.assertEquals(predict, stackedPredict);
    }

    /**
     * Test parameters count works well.
     */
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

    /**
     * Test methods related to parameters flattening.
     */
    @Test
    public void setParamsFlattening() {
        int inputSize = 3;
        int firstLayerNeuronsCnt = 2;
        int secondLayerNeurons = 1;

        DenseVector paramsVector = new DenseVector(new double[] {
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, // First layer weight matrix.
            7.0, 8.0, // Second layer weight matrix.
            9.0 // Second layer biases.
        });

        DenseMatrix firstLayerWeights = new DenseMatrix(new double[][] {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}});
        DenseMatrix secondLayerWeights = new DenseMatrix(new double[][] {{7.0, 8.0}});
        DenseVector secondLayerBiases = new DenseVector(new double[] {9.0});

        MLPArchitecture conf = new MLPArchitecture(inputSize).
            withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID).
            withAddedLayer(secondLayerNeurons, true, Activators.SIGMOID);

        MultilayerPerceptron mlp = new MultilayerPerceptron(conf, new MLPConstInitializer(100, 200));

        mlp.setParameters(paramsVector);
        Assert.assertEquals(paramsVector, mlp.parameters());

        Assert.assertEquals(mlp.weights(1), firstLayerWeights);
        Assert.assertEquals(mlp.weights(2), secondLayerWeights);
        Assert.assertEquals(mlp.biases(2), secondLayerBiases);
    }

    /**
     * Test differentiation.
     */
    @Test
    public void testDifferentiation() {
        int inputSize = 2;
        int firstLayerNeuronsCnt = 1;

        double w10 = 0.1;
        double w11 = 0.2;

        MLPArchitecture conf = new MLPArchitecture(inputSize).
            withAddedLayer(firstLayerNeuronsCnt, false, Activators.SIGMOID);

        MultilayerPerceptron mlp = new MultilayerPerceptron(conf);

        mlp.setWeight(1, 0, 0, w10);
        mlp.setWeight(1, 1, 0, w11);
        double x0 = 1.0;
        double x1 = 3.0;

        Matrix inputs = new DenseMatrix(new double[][] {{x0, x1}}).transpose();
        double ytt = 1.0;
        Matrix truth = new DenseMatrix(new double[][] {{ytt}}).transpose();

        Vector grad = mlp.differentiateByParameters(LossFunctions.MSE, inputs, truth);

        // Let yt be y ground truth value.
        // d/dw1i [(yt - sigma(w10 * x0 + w11 * x1))^2] =
        // 2 * (yt - sigma(w10 * x0 + w11 * x1)) * (-1) * (sigma(w10 * x0 + w11 * x1)) * (1 - sigma(w10 * x0 + w11 * x1)) * xi =
        // let z = sigma(w10 * x0 + w11 * x1)
        // - 2* (yt - z) * (z) * (1 - z) * xi.

        IgniteTriFunction<Double, Vector, Vector, Vector> partialDer = (yt, w, x) -> {
            Double z = Activators.SIGMOID.apply(w.dot(x));

            return x.copy().map(xi -> -2 * (yt - z) * z * (1 - z) * xi);
        };

        Vector weightsVec = mlp.weights(1).getRow(0);
        Tracer.showAscii(weightsVec);

        Vector trueGrad = partialDer.apply(ytt, weightsVec, inputs.getCol(0));

        Tracer.showAscii(trueGrad);
        Tracer.showAscii(grad);

        Assert.assertEquals(mlp.architecture().parametersCount(), grad.size());
        Assert.assertEquals(trueGrad, grad);
    }
}
