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

package org.apache.ignite.ml.nn.trainers.distributed;

import java.util.List;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.updaters.ParameterUpdateCalculator;

/** Multilayer perceptron group update training data. */
public class MLPGroupUpdateTrainingData<U> {
    /** */
    private final ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator;
    /** */
    private final int stepsCnt;
    /** */
    private final IgniteFunction<List<U>, U> updateReducer;
    /** */
    private final IgniteSupplier<IgniteBiTuple<Matrix, Matrix>> batchSupplier;
    /** */
    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;
    /** */
    private final double tolerance;

    /** Construct multilayer perceptron group update training data with all parameters provided. */
    public MLPGroupUpdateTrainingData(
        ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator, int stepsCnt,
        IgniteFunction<List<U>, U> updateReducer,
        IgniteSupplier<IgniteBiTuple<Matrix, Matrix>> batchSupplier,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss, double tolerance) {
        this.updateCalculator = updateCalculator;
        this.stepsCnt = stepsCnt;
        this.updateReducer = updateReducer;
        this.batchSupplier = batchSupplier;
        this.loss = loss;
        this.tolerance = tolerance;
    }

    /** Get update calculator. */
    public ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator() {
        return updateCalculator;
    }

    /** Get count of steps. */
    public int stepsCnt() {
        return stepsCnt;
    }

    /** Get update reducer. */
    public IgniteFunction<List<U>, U> updateReducer() {
        return updateReducer;
    }

    /** Get batch supplier. */
    public IgniteSupplier<IgniteBiTuple<Matrix, Matrix>> batchSupplier() {
        return batchSupplier;
    }

    /** Get loss function. */
    public IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss() {
        return loss;
    }

    /** Get tolerance. */
    public double tolerance() {
        return tolerance;
    }
}
