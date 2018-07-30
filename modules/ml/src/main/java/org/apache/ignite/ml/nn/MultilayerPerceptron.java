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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableDoubleToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.architecture.TransformationLayerArchitecture;
import org.apache.ignite.ml.nn.initializers.MLPInitializer;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.optimization.SmoothParametrized;

import static org.apache.ignite.ml.math.util.MatrixUtil.elementWiseTimes;

/**
 * Class encapsulating logic of multilayer perceptron.
 */
public class MultilayerPerceptron implements Model<Matrix, Matrix>, SmoothParametrized<MultilayerPerceptron>,
    Serializable {
    /**
     * This MLP architecture.
     */
    protected MLPArchitecture architecture;

    /**
     * List containing layers parameters.
     */
    protected List<MLPLayer> layers;

    /**
     * MLP which is 'below' this MLP (i.e. below output goes to this MLP as input).
     */
    protected MultilayerPerceptron below;

    /**
     * Construct MLP from given architecture and parameters initializer.
     *
     * @param arch Architecture.
     * @param initializer Parameters initializer.
     */
    public MultilayerPerceptron(MLPArchitecture arch, MLPInitializer initializer) {
        layers = new ArrayList<>(arch.layersCount() + 1);
        architecture = arch;
        below = null;

        initLayers(initializer != null ? initializer : new RandomInitializer(new Random()));
    }

    /**
     * Construct MLP from given architecture.
     *
     * @param arch Architecture.
     */
    public MultilayerPerceptron(MLPArchitecture arch) {
        this(arch, null);
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
            DenseMatrix weights = new DenseMatrix(neuronsCnt, prevSize);
            initializer.initWeights(weights);
            DenseVector biases = null;
            if (layerCfg.hasBias()) {
                biases = new DenseVector(neuronsCnt);
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
    protected MultilayerPerceptron(MultilayerPerceptron above, MultilayerPerceptron below) {
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
    public Matrix forwardPass(Matrix val, MLPState state, boolean writeState) {
        Matrix res = val;

        if (below != null)
            res = below.forwardPass(val, state, writeState);

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

        return res;
    }

    /**
     * Makes a prediction for the given objects.
     *
     * @param val Matrix containing objects.
     * @return Matrix with predicted vectors.
     */
    @Override public Matrix apply(Matrix val) {
        MLPState state = new MLPState(null);
        forwardPass(val.transpose(), state, false);
        return state.activatorsOutput.get(state.activatorsOutput.size() - 1).transpose();
    }

    /**
     * Create MLP where this MLP output is fed as input to added MLP.
     *
     * @param above Added MLP.
     * @return New MLP where this MLP output is fed as input to added MLP.
     */
    public MultilayerPerceptron add(MultilayerPerceptron above) {
        return new MultilayerPerceptron(above, this);
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
    public MultilayerPerceptron setBiases(int layerIdx, Vector bias) {
        biases(layerIdx).assign(bias);

        return this;
    }

    /**
     * Set the bias of given neuron in given layer.
     *
     * @param layerIdx Layer index.
     * @param neuronIdx Neuron index.
     * @param val New value of bias.
     * @return This MLP with updated biases.
     */
    public MultilayerPerceptron setBias(int layerIdx, int neuronIdx, double val) {
        // Should be transformation layer.
        assert layerIdx > 0;
        assert architecture.transformationLayerArchitecture(layerIdx).hasBias();

        biases(layerIdx).setX(neuronIdx, val);

        return this;
    }

    /**
     * Get the bias of given neuron in given layer.
     *
     * @param layerIdx Layer index.
     * @param neuronIdx Neuron index.
     * @return Bias with specified coordinates.
     */
    public double bias(int layerIdx, int neuronIdx) {
        // Should be transformation layer.
        assert layerIdx > 0;
        assert architecture.transformationLayerArchitecture(layerIdx).hasBias();

        return biases(layerIdx).getX(neuronIdx);
    }

    /**
     * Sets the weighs of layer with a given index.
     *
     * @param layerIdx Layer index.
     * @param weights New values for weights.
     * @return This MLP with updated weights.
     */
    public MultilayerPerceptron setWeights(int layerIdx, Matrix weights) {
        weights(layerIdx).assign(weights);

        return this;
    }

    /**
     * Set the weight of neuron with given index in previous layer to neuron with given index in given layer.
     *
     * @param layerIdx Layer index.
     * @param fromNeuron Neuron index in previous layer.
     * @param toNeuron Neuron index in current layer.
     * @param val New value of weight.
     * @return This MLP with updated weights.
     */
    public MultilayerPerceptron setWeight(int layerIdx, int fromNeuron, int toNeuron, double val) {
        // Should be transformation layer.
        assert layerIdx > 0;

        weights(layerIdx).setX(toNeuron, fromNeuron, val);

        return this;
    }

    /**
     * Get the weight of neuron with given index in previous layer to neuron with given index in given layer.
     *
     * @param layerIdx Layer index.
     * @param fromNeuron Neuron index in previous layer.
     * @param toNeuron Neuron index in current layer.
     * @return Weight with specified coordinates.
     */
    public double weight(int layerIdx, int fromNeuron, int toNeuron) {
        // Should be transformation layer.
        assert layerIdx > 0;
        assert architecture.transformationLayerArchitecture(layerIdx).hasBias();

        return weights(layerIdx).getX(fromNeuron, toNeuron);
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

    /** {@inheritDoc} */
    public Vector differentiateByParameters(IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        Matrix inputsBatch, Matrix truthBatch) {
        // Backpropagation algorithm is used here.
        int batchSize = inputsBatch.columnSize();
        double invBatchSize = 1 / (double)batchSize;
        int lastLayer = layersCount() - 1;
        MLPState mlpState = computeState(inputsBatch);
        Matrix dz = null;

        List<MLPLayer> layersParameters = new LinkedList<>();

        for (int layer = lastLayer; layer > 0; layer--) {
            Matrix z = mlpState.linearOutput(layer).copy();
            Matrix dSigmaDz = differentiateNonlinearity(z,
                architecture().transformationLayerArchitecture(layer).activationFunction());

            if (layer == lastLayer) {
                Matrix sigma = mlpState.activatorsOutput(lastLayer).copy();
                Matrix dLossDSigma = differentiateLoss(truthBatch, sigma, loss);
                dz = elementWiseTimes(dLossDSigma, dSigmaDz);
            }
            else {
                dz = weights(layer + 1).transpose().times(dz);
                dz = elementWiseTimes(dz, dSigmaDz);
            }

            Matrix a = mlpState.activatorsOutput(layer - 1);
            Matrix dw = dz.times(a.transpose()).times(invBatchSize);

            Vector db = null;
            if (hasBiases(layer))
                db = dz.foldRows(Vector::sum).times(invBatchSize);

            // Because we go from last layer, add each layer to the beginning.
            layersParameters.add(0, new MLPLayer(dw, db));
        }

        return paramsAsVector(layersParameters);
    }

    /** {@inheritDoc} */
    public Vector parameters() {
        return paramsAsVector(layers);
    }

    /**
     * Flatten this MLP parameters as vector.
     *
     * @param layersParams List of layers parameters.
     * @return This MLP parameters as vector.
     */
    protected Vector paramsAsVector(List<MLPLayer> layersParams) {
        int off = 0;
        Vector res = new DenseVector(architecture().parametersCount());

        for (MLPLayer layerParams : layersParams) {
            off = writeToVector(res, layerParams.weights, off);

            if (layerParams.biases != null)
                off = writeToVector(res, layerParams.biases, off);
        }

        return res;
    }

    /** {@inheritDoc} */
    public MultilayerPerceptron setParameters(Vector vector) {
        int off = 0;

        for (int l = 1; l < layersCount(); l++) {
            MLPLayer layer = layers.get(l - 1);

            IgniteBiTuple<Integer, Matrix> readRes = readFromVector(vector, layer.weights.rowSize(),
                layer.weights.columnSize(), off);

            off = readRes.get1();
            layer.weights = readRes.get2();

            if (hasBiases(l)) {
                IgniteBiTuple<Integer, Vector> readRes1 = readFromVector(vector, layer.biases.size(), off);
                off = readRes1.get1();

                layer.biases = readRes1.get2();
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public int parametersCount() {
        return architecture().parametersCount();
    }

    /**
     * Read matrix with given dimensions from vector starting with offset and return new offset position
     * which is last matrix entry position + 1.
     *
     * @param v Vector to read from.
     * @param rows Count of rows of matrix to read.
     * @param cols Count of columns of matrix to read.
     * @param off Start read position.
     * @return New offset position which is last matrix entry position + 1.
     */
    private IgniteBiTuple<Integer, Matrix> readFromVector(Vector v, int rows, int cols, int off) {
        Matrix mtx = new DenseMatrix(rows, cols);

        int size = rows * cols;
        for (int i = 0; i < size; i++)
            mtx.setX(i / cols, i % cols, v.getX(off + i));

        return new IgniteBiTuple<>(off + size, mtx);
    }

    /**
     * Read vector of given size from vector and return new offset position which is last read vector entry position + 1.
     *
     * @param v Vector to read from.
     * @param size Size of vector to read.
     * @param off Start read position.
     * @return New offset position which is last read vector entry position + 1.
     */
    private IgniteBiTuple<Integer, Vector> readFromVector(Vector v, int size, int off) {
        Vector vec = new DenseVector(size);

        for (int i = 0; i < size; i++)
            vec.setX(i, v.getX(off + i));

        return new IgniteBiTuple<>(off + size, vec);
    }

    /**
     * Write matrix into vector starting from offset and return new offset position which is last written entry position + 1.
     *
     * @param vec Vector to write into.
     * @param mtx Matrix to write.
     * @param off Start write position.
     * @return New offset position which is last written entry position + 1.
     */
    private int writeToVector(Vector vec, Matrix mtx, int off) {
        int rows = mtx.rowSize();
        int cols = mtx.columnSize();

        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                vec.setX(off, mtx.getX(r, c));
                off++;
            }
        }

        return off;
    }

    /**
     * Write vector into vector starting from offset and return new offset position which is last written entry position + 1.
     *
     * @param vec Vector to write into.
     * @param v Vector to write.
     * @param off Start write position.
     * @return New offset position which is last written entry position + 1.
     */
    private int writeToVector(Vector vec, Vector v, int off) {
        for (int i = 0; i < v.size(); i++) {
            vec.setX(off, v.getX(i));
            off++;
        }

        return off;
    }

    /**
     * Differentiate loss.
     *
     * @param groundTruth Ground truth values.
     * @param lastLayerOutput Last layer output.
     * @param loss Loss function.
     * @return Gradients matrix.
     */
    private Matrix differentiateLoss(Matrix groundTruth, Matrix lastLayerOutput,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        Matrix diff = groundTruth.like(groundTruth.rowSize(), groundTruth.columnSize());

        for (int col = 0; col < groundTruth.columnSize(); col++) {
            // TODO: IGNITE-7155 Couldn't use views here because copy on views doesn't do actual copy and all changes are propagated to original.
            Vector gtCol = groundTruth.getCol(col);
            Vector predCol = lastLayerOutput.getCol(col);
            diff.assignColumn(col, loss.apply(gtCol).differential(predCol));
        }

        return diff;
    }

    /**
     * Differentiate nonlinearity.
     *
     * @param linearOut Linear output of current layer.
     * @param nonlinearity Nonlinearity of current layer.
     * @return Gradients matrix.
     */
    private Matrix differentiateNonlinearity(Matrix linearOut,
        IgniteDifferentiableDoubleToDoubleFunction nonlinearity) {
        Matrix diff = linearOut.copy();

        diff.map(nonlinearity::differential);

        return diff;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        StringBuilder builder = new StringBuilder("MultilayerPerceptron [\n");
        if(below != null)
            builder.append("below = \n").append(below.toString(pretty)).append("\n\n");
        builder.append("layers = [").append(pretty ? "\n" : "");
        for(int i = 0; i < layers.size(); i++) {
            MLPLayer layer = layers.get(i);
            builder.append("\tlayer").append(i).append(" = [\n");
            if(layer.biases != null)
                builder.append("\t\tbias = ").append(Tracer.asAscii(layer.biases, "%.4f", false)).append("\n");
            String matrix = Tracer.asAscii(layer.weights, "%.4f").replaceAll("\n", "\n\t\t\t");
            builder.append("\t\tweights = [\n\t\t\t").append(matrix).append("\n\t\t]");
            builder.append("\n\t]\n");
        }
        builder.append("]");
        return builder.toString();
    }
}
