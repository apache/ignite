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
import org.apache.ignite.ml.math.functions.IgniteDifferentiableDoubleToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.MLP;
import org.apache.ignite.ml.nn.MLPState;

/**
 * Adapter of {@link MLPParameterUpdater} for updaters performing backpropagation updates of MLP parameters.
 */
public abstract class BackpropUpdater<P extends UpdaterParams> implements MLPParameterUpdater<P> {
    /**
     * Losses function.
     */
    protected IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Learning rate.
     */
    protected double learningRate;

    /**
     * State of MLP.
     */
    protected MLPState mlpState;


    /** {@inheritDoc} */
    @Override public final P init(MLP mlp, double learningRate,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;
        this.learningRate = learningRate;

        return initParameters(mlp);
    }

    protected abstract P initParameters(MLP mlp);

    /** {@inheritDoc} */
    @Override public final P updateParams(MLP mlp, P updaterParams, int iteration,
        Matrix inputs, Matrix groundTruth) {
        int lastLayer = mlp.layersCount() - 1;
        int batchSize = groundTruth.columnSize();

        int layer = lastLayer;

        initIterationData(mlp, updaterParams, iteration, learningRate, inputs);

        while (layer > 0) {
            updaterParams = updateLayerData(mlp, updaterParams, layer, inputs, groundTruth);
            layer--;
        }

        return updaterParams;
    }

    /** {@inheritDoc} */
    protected void initIterationData(MLP mlp, P updaterParams, int iteration,  double learningRate, Matrix inputs) {
        this.mlpState = mlp.computeState(inputs);
    }

    protected abstract P updateLayerData(MLP mlp, P updaterParams, int layer, Matrix inputs, Matrix groundTruth);

    protected Matrix differentiateLoss(Matrix groundTruth, Matrix lastLayerOutput, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        Matrix diff = groundTruth.like(groundTruth.rowSize(), groundTruth.columnSize());

        for (int col = 0; col < groundTruth.columnSize(); col++) {
            // TODO: IGNITE-7155 Couldn't use views here because copy on views doesn't do actual copy and all changes are propagated to original.
            Vector gtCol = groundTruth.getCol(col);
            Vector predCol = lastLayerOutput.getCol(col);
            diff.assignColumn(col, loss.apply(gtCol).differential(predCol));
        }

        return diff;
    }

    protected Matrix differentiateNonlinearity(Matrix linearOut, IgniteDifferentiableDoubleToDoubleFunction nonlinearity) {
        Matrix diff = linearOut.copy();

        diff.map(nonlinearity::differential);

        return diff;
    }
}
