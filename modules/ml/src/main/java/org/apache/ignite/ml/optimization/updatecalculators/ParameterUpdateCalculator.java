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

package org.apache.ignite.ml.optimization.updatecalculators;

import java.io.Serializable;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Interface for classes encapsulating parameters updateCache logic.
 *
 * @param <M> Type of model to be updated.
 * @param <P> Type of parameters needed for this update calculator.
 */
public interface ParameterUpdateCalculator<M, P extends Serializable> extends Serializable {
    /**
     * Initializes the update calculator.
     *
     * @param mdl Model to be trained.
     * @param loss Loss function.
     * @return Initialized parameters.
     */
    public P init(M mdl, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss);

    /**
     * Calculate new update.
     *
     * @param mdl Model to be updated.
     * @param updaterParameters Updater parameters to updateCache.
     * @param iteration Current trainer iteration.
     * @param inputs Inputs.
     * @param groundTruth True values.
     * @return Updated parameters.
     */
    public P calculateNewUpdate(M mdl, P updaterParameters, int iteration, Matrix inputs, Matrix groundTruth);

    /**
     * Update given obj with this parameters.
     *
     * @param obj Object to be updated.
     */
    public <M1 extends M> M1 update(M1 obj, P update);
}
