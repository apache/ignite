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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.nn.initializers.MLPInitializer;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.architecture.TransformationLayerArchitecture;

/**
 * Class encapsulating logic of multilayer perceptron.
 */
public class MLP implements Model<Matrix, Matrix> {
    /**
     * This MLP architecture.
     */
    protected MLPArchitecture architecture;

    /**
     * List containing layers parameters.
     */
    protected List<MLPLayer> layers;

    /**
     * MLP which is 'under' this MLP (i.e.) below output goes to this MLP as input.
     */
    protected MLP below;

    /**
     * Construct MLP from given architecture and parameters initializer.
     *
     * @param arch Architecture.
     * @param initializer Parameters initializer.
     */
    public MLP(MLPArchitecture arch, MLPInitializer initializer) {
        layers = new ArrayList<>(arch.layersCount() + 1);
        architecture = arch;
        below = null;

        initLayers(initializer);
    }

    /**
     * Construct MLP from given architecture.
     *
     * @param arch Architecture.
     */
    public MLP(MLPArchitecture arch) {
        layers = new ArrayList<>(arch.layersCount() + 1);
        architecture = arch;
        below = null;

        initLayers(new RandomInitializer(new Random()));
    }

    /**
     * Init layers parameters with initializer.
     *
     * @param initializer Parameters initializer.
     */
    private void initLayers(MLPInitializer initializer) {
        int prevSize = architecture.inputSize();

        for (int i = 1; i < architecture.layersCount(); i++) {
            TransformationLayerArchitecture layerCfg = architecture.transformationLayerArchitecture(i);
            int neuronsCnt = layerCfg.neuronsCount();
            DenseLocalOnHeapMatrix weights = new DenseLocalOnHeapMatrix(neuronsCnt, prevSize);
            initializer.initWeights(weights);
            DenseLocalOnHeapVector biases = null;
            if (layerCfg.hasBias()) {
                biases = new DenseLocalOnHeapVector(neuronsCnt);
                initializer.initBiases(biases);
            }
            layers.add(new MLPLayer(weights, biases));
            prevSize = layerCfg.neuronsCount();
        }
    }

    /**
     * Create MLP from two MLPs: first stacked on second.
     *
     * @param above MLP to be above.
     * @param below MLP to be below.
     */
    protected MLP(MLP above, MLP below) {
        this.layers = above.layers;
        this.architecture = above.architecture;
        this.below = below;
    }

    /**
     * Perform forward pass and return state of outputs of each layer.
     *
     * @param val Value to perform computation on.
     * @return MLP state after computation.
     */
    public MLPState computeState(Matrix val) {
        MLPState res = new MLPState(val);

        forwardPass(val, res, true);

        return res;
    }

    /**
     * Perform forward pass and if needed write state of outputs of each layer.
     *
     * @param val Value to perform computation on.
     * @param state State object to write state into.
     * @param writeState Flag indicating need to write state.
     */
    public void forwardPass(Matrix val, MLPState state, boolean writeState) {
        Matrix res = val;

        if (below != null)
            below.forwardPass(val, state, writeState);

        for (int i = 1; i < architecture.layersCount(); i++) {
            MLPLayer curLayer = layers.get(i - 1);
            res = curLayer.weights.times(res);

            TransformationLayerArchitecture layerCfg = this.architecture.transformationLayerArchitecture(i);

            if (layerCfg.hasBias()) {
                ReplicatedVectorMatrix biasesMatrix = new ReplicatedVectorMatrix(biases(i), res.columnSize(), true);
                res = res.plus(biasesMatrix);
            }

            state.linearOutput.add(res);

            // If we do not write state, we can overwrite result.
            if (writeState)
                res = res.copy();

            res = res.map(layerCfg.activationFunction());

            state.activatorsOutput.add(res);
        }
    }

    /**
     * Predict values on inputs given as columns in a given matrix.
     *
     * @param val Matrix containing inputs as columns.
     * @return Matrix with predicted vectors stored in columns with column indexes corresponding to column indexes in
     * the input matrix.
     */
    @Override public Matrix predict(Matrix val) {
        MLPState state = new MLPState(null);
        forwardPass(val, state, false);
        return state.activatorsOutput.get(state.activatorsOutput.size() - 1);
    }

    /**
     * Create MLP where this MLP output is fed as input to added MLP.
     *
     * @param above Added MLP.
     * @return New MLP where this MLP output is fed as input to added MLP.
     */
    public MLP add(MLP above) {
        return new MLP(above, this);
    }

    /**
     * Get weights of layer with given index. Proper indexes are in [1, layersCount).
     *
     * @param layerIdx Layer index.
     * @return Weights of layer with given index.
     */
    public Matrix weights(int layerIdx) {
        assert layerIdx >= 1;
        assert layerIdx < architecture.layersCount() || below != null;

        if (layerIdx < belowLayersCount())
            return below.weights(layerIdx - architecture.layersCount());
        else
            return layers.get(layerIdx - belowLayersCount() - 1).weights;
    }

    /**
     * Get biases of layer with given index. Proper indexes are in [1, layersCount).
     *
     * @param layerIdx Layer index.
     * @return Biases of layer with given index.
     */
    public Vector biases(int layerIdx) {
        assert layerIdx >= 0;
        assert layerIdx < architecture.layersCount() || below != null;

        if (layerIdx < belowLayersCount())
            return below.biases(layerIdx - architecture.layersCount());
        else
            return layers.get(layerIdx - belowLayersCount() - 1).biases;
    }

    /**
     * Checks if layer with given index has biases.
     *
     * @param layerIdx Layer index.
     * @return Value of predicate 'layer with layerIdx has biases'.
     */
    public boolean hasBiases(int layerIdx) {
        return layerIdx != 0 && biases(layerIdx) != null;

    }

    /**
     * Sets the biases of layer with a given index.
     *
     * @param layerIdx Layer index.
     * @param bias New values for biases.
     * @return This MLP with updated biases.
     */
    public MLP setBiases(int layerIdx, Vector bias) {
        biases(layerIdx).assign(bias);

        return this;
    }

    /**
     * Sets the bias of given neuron in given layer.
     *
     * @param layerIdx Layer index.
     * @param neuronIdx Neuron index.
     * @param val New value of bias.
     * @return This MLP with updated biases.
     */
    public MLP setBias(int layerIdx, int neuronIdx, double val) {
        // Should be transformation layer.
        assert layerIdx > 0;
        assert architecture.transformationLayerArchitecture(layerIdx).hasBias();

        biases(layerIdx).setX(neuronIdx, val);

        return this;
    }

    /**
     * Sets the weighs of layer with a given index.
     *
     * @param layerIdx Layer index.
     * @param weights New values for weights.
     * @return This MLP with updated weights.
     */
    public MLP setWeights(int layerIdx, Matrix weights) {
        weights(layerIdx).assign(weights);

        return this;
    }

    /**
     * Sets the weight of neuron with given index of previous layer.
     *
     * @param layerIdx Layer index.
     * @param fromNeuron Neuron index in previous layer.
     * @param val New value of weight.
     * @return This MLP with updated weights.
     */
    public MLP setWeight(int layerIdx, int fromNeuron, int toNeuron, double val) {
        // Should be transformation layer.
        assert layerIdx > 0;
        assert architecture.transformationLayerArchitecture(layerIdx).hasBias();

        weights(layerIdx).setX(fromNeuron, toNeuron, val);

        return this;
    }

    /**
     * Get count of layers in this MLP.
     *
     * @return Count of layers in this MLP.
     */
    public int layersCount() {
        return architecture.layersCount() + (below != null ? below.layersCount() : 0);
    }

    /** Count of layers in below MLP. */
    protected int belowLayersCount() {
        return below != null ? below.layersCount() : 0;
    }

    /**
     * Get architecture of this MLP.
     *
     * @return Architecture of this MLP.
     */
    public MLPArchitecture architecture() {
        if (below != null)
            return below.architecture().add(architecture());
        return architecture;
    }
}
