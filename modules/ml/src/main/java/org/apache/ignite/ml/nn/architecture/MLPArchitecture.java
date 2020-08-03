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

package org.apache.ignite.ml.nn.architecture;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableDoubleToDoubleFunction;

/**
 * Class containing information about architecture of MLP.
 */
public class MLPArchitecture implements Serializable {
    /**
     * List of layers architectures.
     */
    private final List<LayerArchitecture> layers;

    /**
     * Construct an MLP architecture.
     *
     * @param inputSize Size of input to MLP.
     */
    public MLPArchitecture(int inputSize) {
        layers = new ArrayList<>();
        layers.add(new LayerArchitecture(inputSize));
    }

    /**
     * Construct an MLP architecture.
     *
     * @param layers List of layers architectures.
     */
    private MLPArchitecture(List<LayerArchitecture> layers) {
        this.layers = layers;
    }

    /**
     * Count of layers in MLP.
     *
     * @return Layers count.
     */
    public int layersCount() {
        return layers.size();
    }

    /**
     * Size of input of MLP.
     *
     * @return Size of input.
     */
    public int inputSize() {
        return layers.get(0).neuronsCount();
    }

    /**
     * Size of output of MLP.
     *
     * @return Size of output.
     */
    public int outputSize() {
        return layers.get(layersCount() - 1).neuronsCount();
    }

    /**
     * Constructs new MLP architecture with new layer added on top of all this architecture layers.
     *
     * @param neuronsCnt Count of neurons in new layer.
     * @param hasBias Flag indicating presence of bias in added layer.
     * @param f Activation function of a new layer.
     * @return New MLP architecture with new layer added on top of all this architecture layers.
     */
    public MLPArchitecture withAddedLayer(int neuronsCnt, boolean hasBias,
        IgniteDifferentiableDoubleToDoubleFunction f) {
        ArrayList<LayerArchitecture> newLayers = new ArrayList<>(layers);

        newLayers.add(new TransformationLayerArchitecture(neuronsCnt, hasBias, f));

        return new MLPArchitecture(newLayers);
    }

    /**
     * Get architecture of layer with given index.
     *
     * @param layer Index of layer to get architecture from.
     * @return Architecture of layer with given index.
     */
    public LayerArchitecture layerArchitecture(int layer) {
        return layers.get(layer);
    }

    /**
     * Get architecture of transformation layer (i.e. non-input layer) with given index.
     *
     * @param layer Index of layer to get architecture from.
     * @return Architecture of transformation layer with given index.
     */
    public TransformationLayerArchitecture transformationLayerArchitecture(int layer) {
        return (TransformationLayerArchitecture)layers.get(layer);
    }

    /**
     * Creates config describing network where first goes this config and after goes this method's argument.
     *
     * @param second Config to add after this config.
     * @return New combined configuration.
     */
    public MLPArchitecture add(MLPArchitecture second) {
        assert second.inputSize() == outputSize();

        MLPArchitecture res = new MLPArchitecture(inputSize());
        res.layers.addAll(layers);
        res.layers.addAll(second.layers);

        return res;
    }

    /**
     * Count of parameters in this MLP architecture.
     *
     * @return Parameters in this MLP architecture.
     */
    public int parametersCount() {
        int res = 0;

        for (int i = 1; i < layersCount(); i++) {
            TransformationLayerArchitecture la = transformationLayerArchitecture(i);
            res += layerArchitecture(i - 1).neuronsCount() * la.neuronsCount();

            if (la.hasBias())
                res += la.neuronsCount();

        }

        return res;
    }
}
