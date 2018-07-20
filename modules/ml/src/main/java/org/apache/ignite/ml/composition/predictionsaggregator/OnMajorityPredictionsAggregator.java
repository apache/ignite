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

package org.apache.ignite.ml.composition.predictionsaggregator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Predictions aggregator returning the most frequently prediction.
 */
public class OnMajorityPredictionsAggregator implements PredictionsAggregator {
    /** {@inheritDoc} */
    @Override public Double apply(double[] estimations) {
        A.notEmpty(estimations, "estimations vector");

        Map<Double, Integer> cntrsByCls = new HashMap<>();

        for (Double predictedValue : estimations) {
            Integer cntrVal = cntrsByCls.getOrDefault(predictedValue, 0) + 1;
            cntrsByCls.put(predictedValue, cntrVal);
        }

        return cntrsByCls.entrySet().stream()
            .max(Comparator.comparing(Map.Entry::getValue))
            .get().getKey();
    }
}
