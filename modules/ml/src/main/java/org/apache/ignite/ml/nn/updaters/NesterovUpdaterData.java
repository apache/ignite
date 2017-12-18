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

/**
 * Data needed for Nesterov parameters updater.
 */
public class NesterovUpdaterData {
    /**
     * Previous step weights updates.
     */
    protected Matrix[] prevIterationWeightsUpdates;

    /**
     * Previous step biases updates.
     */
    protected Vector[] prevIterationBiasesUpdates;

    /**
     * Construct NesterovUpdaterData.
     *
     * @param layersCnt Count of layers on which update happens.
     */
    public NesterovUpdaterData(int layersCnt) {
        prevIterationWeightsUpdates = new Matrix[layersCnt];
        prevIterationBiasesUpdates = new Vector[layersCnt];
    }

    /**
     * Set previous step weights updates for layer with given index.
     *
     * @param layerIdx Layer index.
     * @param weightsUpdates Weights updates.
     * @return This object with updated weights updates.
     */
    public NesterovUpdaterData setPreviousWeights(int layerIdx, Matrix weightsUpdates) {
        prevIterationWeightsUpdates[layerIdx] = weightsUpdates;
        return this;
    }

    /**
     * Set previous step biases updates for layer with given index.
     *
     * @param layerIdx Layer index.
     * @param biasesUpdates Biases updates.
     * @return This object with updated biases updates.
     */
    public NesterovUpdaterData setPreviousBiases(int layerIdx, Vector biasesUpdates) {
        prevIterationBiasesUpdates[layerIdx] = biasesUpdates;
        return this;
    }
}
