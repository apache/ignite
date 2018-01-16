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

package org.apache.ignite.ml.trainers.group;

import java.util.List;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.optimization.updatecalculators.ParameterUpdateCalculator;

public class UpdatesStrategy<M, U> {
    private ParameterUpdateCalculator<M, U> updatesCalculator;

    private IgniteFunction<List<U>, U> locStepUpdatesReducer;

    private IgniteFunction<List<U>, U> allUpdatesReducer;

    public UpdatesStrategy(
        ParameterUpdateCalculator<M, U> updatesCalculator,
        IgniteFunction<List<U>, U> locStepUpdatesReducer,
        IgniteFunction<List<U>, U> allUpdatesReducer) {
        this.updatesCalculator = updatesCalculator;
        this.locStepUpdatesReducer = locStepUpdatesReducer;
        this.allUpdatesReducer = allUpdatesReducer;
    }

    public IgniteFunction<List<U>, U> locStepUpdatesReducer() {
        return locStepUpdatesReducer;
    }

    public IgniteFunction<List<U>, U> allUpdatesReducer() {
        return allUpdatesReducer;
    }

    public ParameterUpdateCalculator<M, U> getUpdatesCalculator() {
        return updatesCalculator;
    }
}
