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

package org.apache.ignite.ml.nn;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.optimization.updatecalculators.ParameterUpdateCalculator;

/**
 * Class encapsulating update strategies for group trainers based on updates.
 *
 * @param <M> Type of model to be optimized.
 * @param <U> Type of update.
 */
public class UpdatesStrategy<M, U extends Serializable> {
    /**
     * {@link ParameterUpdateCalculator}.
     */
    private ParameterUpdateCalculator<M, U> updatesCalculator;

    /**
     * Function used to reduce updates in one training (for example, sum all sequential gradient updates to get one
     * gradient update).
     */
    private IgniteFunction<List<U>, U> locStepUpdatesReducer;

    /**
     * Function used to reduce updates from different trainings (for example, averaging of gradients of all parallel trainings).
     */
    private IgniteFunction<List<U>, U> allUpdatesReducer;

    /**
     * Construct instance of this class with given parameters.
     *
     * @param updatesCalculator Parameter update calculator.
     * @param locStepUpdatesReducer Function used to reduce updates in one training
     * (for example, sum all sequential gradient updates to get one gradient update).
     * @param allUpdatesReducer Function used to reduce updates from different trainings
     * (for example, averaging of gradients of all parallel trainings).
     */
    public UpdatesStrategy(
        ParameterUpdateCalculator<M, U> updatesCalculator,
        IgniteFunction<List<U>, U> locStepUpdatesReducer,
        IgniteFunction<List<U>, U> allUpdatesReducer) {
        this.updatesCalculator = updatesCalculator;
        this.locStepUpdatesReducer = locStepUpdatesReducer;
        this.allUpdatesReducer = allUpdatesReducer;
    }

    /**
     * Get parameter update calculator (see {@link ParameterUpdateCalculator}).
     *
     * @return Parameter update calculator.
     */
    public ParameterUpdateCalculator<M, U> getUpdatesCalculator() {
        return updatesCalculator;
    }

    /**
     * Get function used to reduce updates in one training
     * (for example, sum all sequential gradient updates to get one gradient update).
     *
     * @return Function used to reduce updates in one training
     * (for example, sum all sequential gradient updates to get on gradient update).
     */
    public IgniteFunction<List<U>, U> locStepUpdatesReducer() {
        return locStepUpdatesReducer;
    }

    /**
     * Get function used to reduce updates from different trainings
     * (for example, averaging of gradients of all parallel trainings).
     *
     * @return Function used to reduce updates from different trainings.
     */
    public IgniteFunction<List<U>, U> allUpdatesReducer() {
        return allUpdatesReducer;
    }
}
