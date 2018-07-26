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

package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Interface for models which are smooth functions of their parameters.
 */
public interface SmoothParametrized<M extends Parametrized<M>> extends Parametrized<M>, Model<Matrix, Matrix> {
    /**
     * Compose function in the following way: feed output of this model as input to second argument to loss function.
     * After that we have a function g of three arguments: input, ground truth, parameters.
     * If we consider function
     * h(w) = 1 / M sum_{i=1}^{M} g(w, input_i, groundTruth_i),
     * where M is number of entries in batch, we get function of one argument: parameters vector w.
     * This function is being differentiated.
     *
     * @param loss Loss function.
     * @param inputsBatch Batch of inputs.
     * @param truthBatch Batch of ground truths.
     * @return Gradient of h at current point in parameters space.
     */
    public Vector differentiateByParameters(IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        Matrix inputsBatch, Matrix truthBatch);
}
