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
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.MLP;

public class Gradients implements UpdaterParams {
    private Matrix[] dw;

    private Vector[] db;

    public Gradients(int layersCount) {
        dw = new Matrix[layersCount - 1];
        db = new Vector[layersCount - 1];
    }

    public Matrix weightsGradients(int layerIdx) {
        return dw[layerIdx - 1];
    }

    public void setWeightGradients(int layerIdx, Matrix dw) {
        this.dw[layerIdx - 1] = dw;
    }

    public Vector biasGradients(int layerIdx) {
        return db[layerIdx - 1];
    }

    public void setBiasGradients(int layerIdx, Vector db) {
        this.db[layerIdx - 1] = db;
    }

    @Override public void updateMLP(MLP mlp) {
        for (int layer = 1; layer < mlp.layersCount(); layer++) {
            MatrixUtil.elementWiseMinus(mlp.weights(layer), weightsGradients(layer));
            VectorUtils.elementWiseMinus(mlp.biases(layer), biasGradients(layer));
        }
    }
}
