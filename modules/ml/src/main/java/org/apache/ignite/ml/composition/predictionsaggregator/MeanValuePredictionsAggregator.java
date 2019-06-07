/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.composition.predictionsaggregator;

import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Predictions aggregator returning the mean value of predictions.
 */
public final class MeanValuePredictionsAggregator implements PredictionsAggregator {
    /** {@inheritDoc} */
    @Override public Double apply(double[] estimations) {
        A.notEmpty(estimations, "estimations vector");
        return Arrays.stream(estimations).reduce(0.0, Double::sum) / estimations.length;
    }
}
