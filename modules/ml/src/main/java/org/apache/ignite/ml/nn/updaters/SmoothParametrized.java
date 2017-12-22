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
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Interface for models which are smooth functions of their parameters.
 */
public interface SmoothParametrized {
    /**
     * Compose function in the following way: feed output of this model as input to second argument to loss.
     * After that we have a function of three arguments: input, ground truth, parameters.
     * If we fix ground truths values and inputs, we get function of one argument: parameters vector.
     * This function is being differentiated.
     *
     * @param loss Loss function.
     * @param inputsBatch Batch of inputs.
     * @param truthBatch Batch of ground truths.
     * @return
     */
    Vector differentiateByParameters(IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss, Matrix inputsBatch, Matrix truthBatch);

    /**
     * Get parameters vector.
     *
     * @return Parameters vector.
     */
    Vector parameters();

    /**
     * Set parameters.
     *
     * @param vector Parameters vector.
     */
    void setParameters(Vector vector);

    /**
     * Get count of parameters of this model.
     *
     * @return Count of parameters of this model.
     */
    int parametersCount();
}
