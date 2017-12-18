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

package org.apache.ignite.ml.nn.updaters;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOffHeapVector;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.architecture.TransformationLayerArchitecture;

/**
 * Data needed for RProp updater.
 * @see <a href="https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf">https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf</a>.
 */
class RPropUpdaterData {
    /**
     * Previous iteration weights updates. In original paper they are labeled with "delta w".
     */
    protected Matrix[] prevIterationWeightsUpdates;

    /**
     * Previous iteration weights updates. In original paper they are labeled with "delta w".
     * (in the original paper there is no distinction between weights and biases).
     */
    protected Vector[] prevIterationBiasesUpdates;

    /**
     * Previous iteration weights partial derivatives by weights.
     */
    protected Matrix[] prevIterationWeightsDerivatives;

    /**
     * Previous iteration biases partial derivatives by biases.
     */
    protected Vector[] prevIterationBiasesDerivatives;

    /**
     * Previous iteration weights deltas. In original paper they are labeled with "delta".
     */
    protected Matrix[] weightDeltas;

    /**
     * Previous iteration biases deltas. In original paper they are labeled with "delta".
     */
    protected Vector[] biasDeltas;

    /**
     * Construct RPropUpdaterData.
     *
     * @param arch MLP architecture.
     * @param initUpdate Initial update (in original work labeled as "delta_0").
     */
    RPropUpdaterData(MLPArchitecture arch, double initUpdate) {
        prevIterationWeightsUpdates = new Matrix[arch.layersCount() - 1];
        prevIterationBiasesUpdates = new Vector[arch.layersCount() - 1];

        prevIterationWeightsDerivatives = new Matrix[arch.layersCount() - 1];
        prevIterationBiasesDerivatives = new Vector[arch.layersCount() - 1];

        weightDeltas = new Matrix[arch.layersCount() - 1];
        biasDeltas = new Vector[arch.layersCount() - 1];

        for (int layer = 1; layer < arch.layersCount(); layer++) {
            TransformationLayerArchitecture curLayerArch = arch.transformationLayerArchitecture(layer);
            int rowSize = curLayerArch.neuronsCount();
            int colSize = arch.layerArchitecture(layer - 1).neuronsCount();

            weightDeltas[layer - 1] = new DenseLocalOnHeapMatrix(rowSize, colSize);
            weightDeltas[layer - 1].assign(initUpdate);

            if (curLayerArch.hasBias())
                biasDeltas[layer - 1] = new DenseLocalOffHeapVector(rowSize).assign(initUpdate);
        }
    }

    /**
     * Get weight deltas.
     *
     * @param layer Layer index.
     * @return Weight deltas.
     */
    Matrix weightDeltas(int layer) {
        return weightDeltas[layer - 1];
    }

    /**
     * Get bias deltas.
     *
     * @param layer Layer index.
     * @return Bias deltas.
     */
    Vector biasDeltas(int layer) {
        return biasDeltas[layer - 1];
    }

    /**
     * Get previous iteration weights updates. In original paper they are labeled with "delta w".
     *
     * @param layer Layer index.
     * @return Weights updates.
     */
    Matrix prevIterationWeightsUpdates(int layer) {
        return prevIterationWeightsUpdates[layer - 1];
    }

    /**
     * Set previous iteration weights updates. In original paper they are labeled with "delta w".
     *
     * @param layer Layer index.
     * @param weightsUpdates New weights updates value.
     * @return This object.
     */
    RPropUpdaterData setPrevIterationWeightsUpdates(int layer, Matrix weightsUpdates) {
        prevIterationWeightsUpdates[layer - 1] = weightsUpdates;
        return this;
    }

    /**
     * Get previous iteration biases updates. In original paper they are labeled with "delta w".
     *
     * @param layer Layer index.
     * @return Biases updates.
     */
    Vector prevIterationBiasesUpdates(int layer) {
        return prevIterationBiasesUpdates[layer - 1];
    }

    /**
     * Set previous iteration biases updates. In original paper they are labeled with "delta w".
     *
     * @param layer Layer index.
     * @param biasesUpdates New biases updates value.
     * @return This object.
     */
    Vector setPrevIterationBiasesUpdates(int layer, Vector biasesUpdates) {
        return prevIterationBiasesUpdates[layer - 1] = biasesUpdates;
    }

    /**
     * Get previous iteration loss function partial derivatives by weights.
     *
     * @param layer Layer index.
     * @return Previous iteration loss function partial derivatives by weights.
     */
    Matrix prevIterationWeightsDerivatives(int layer) {
        return prevIterationWeightsDerivatives[layer - 1];
    }

    /**
     * Set previous iteration loss function partial derivatives by weights.
     *
     * @param layer Layer index.
     * @return This object.
     */
    RPropUpdaterData setPrevIterationWeightsDerivatives(int layer, Matrix weightsDerivatives) {
        prevIterationWeightsDerivatives[layer - 1] = weightsDerivatives;
        return this;
    }

    /**
     * Get previous iteration loss function partial derivatives by biases.
     *
     * @param layer Layer index.
     * @return Previous iteration loss function partial derivatives by biases.
     */
    Vector prevIterationBiasesDerivatives(int layer) {
        return prevIterationBiasesDerivatives[layer - 1];
    }

    /**
     * Set previous iteration loss function partial derivatives by biases.
     *
     * @param layer Layer index.
     * @return This object.
     */
    RPropUpdaterData setPrevIterationBiasesDerivatives(int layer, Vector biasesDerivatives) {
        prevIterationBiasesDerivatives[layer - 1] = biasesDerivatives;
        return this;
    }
}
