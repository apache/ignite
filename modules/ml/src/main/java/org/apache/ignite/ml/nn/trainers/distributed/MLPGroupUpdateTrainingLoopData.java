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

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.optimization.updatecalculators.ParameterUpdateCalculator;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;

/** Multilayer perceptron group update training loop data. */
public class MLPGroupUpdateTrainingLoopData<P> implements Serializable {
    /** {@link ParameterUpdateCalculator}. */
    private final ParameterUpdateCalculator<? super MultilayerPerceptron, P> updateCalculator;

    /**
     * Count of steps which should be done by each of parallel trainings before sending it's update for combining with
     * other parallel trainings updates.
     */
    private final int stepsCnt;

    /** Function used to reduce updates of all steps of given parallel training. */
    private final IgniteFunction<List<P>, P> updateReducer;

    /** Previous update. */
    private final P previousUpdate;

    /** Supplier of batches. */
    private final IgniteSupplier<IgniteBiTuple<Matrix, Matrix>> batchSupplier;

    /** Loss function. */
    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /** Error tolerance. */
    private final double tolerance;

    /** Key. */
    private final GroupTrainerCacheKey<Void> key;

    /** MLP. */
    private final MultilayerPerceptron mlp;

    /** Create multilayer perceptron group update training loop data. */
    public MLPGroupUpdateTrainingLoopData(MultilayerPerceptron mlp,
        ParameterUpdateCalculator<? super MultilayerPerceptron, P> updateCalculator, int stepsCnt,
        IgniteFunction<List<P>, P> updateReducer, P previousUpdate,
        GroupTrainerCacheKey<Void> key, IgniteSupplier<IgniteBiTuple<Matrix, Matrix>> batchSupplier,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        double tolerance) {
        this.mlp = mlp;
        this.updateCalculator = updateCalculator;
        this.stepsCnt = stepsCnt;
        this.updateReducer = updateReducer;
        this.previousUpdate = previousUpdate;
        this.key = key;
        this.batchSupplier = batchSupplier;
        this.loss = loss;
        this.tolerance = tolerance;
    }

    /** Get perceptron. */
    public MultilayerPerceptron mlp() {
        return mlp;
    }

    /** Get update calculator. */
    public ParameterUpdateCalculator<? super MultilayerPerceptron, P> updateCalculator() {
        return updateCalculator;
    }

    /** Get steps count. */
    public int stepsCnt() {
        return stepsCnt;
    }

    /** Get update reducer. */
    public IgniteFunction<List<P>, P> getUpdateReducer() {
        return updateReducer;
    }

    /** Get previous update. */
    public P previousUpdate() {
        return previousUpdate;
    }

    /** Get group trainer cache key. */
    public GroupTrainerCacheKey<Void> key() {
        return key;
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
