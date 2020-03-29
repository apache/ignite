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
package org.apache.ignite.ml.naivebayes.gaussian;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.util.MapUtil;

/** Service class is used to calculate means and variances. */
class GaussianNaiveBayesSumsHolder implements Serializable, AutoCloseable {
    /** Serial version uid. */
    private static final long serialVersionUID = 1L;

    /** Sum of all values for all features for each label */
    Map<Double, double[]> featureSumsPerLbl = new HashMap<>();

    /** Sum of all squared values for all features for each label */
    Map<Double, double[]> featureSquaredSumsPerLbl = new HashMap<>();

    /** Rows count for each label */
    Map<Double, Integer> featureCountersPerLbl = new HashMap<>();

    /** Merge to current */
    GaussianNaiveBayesSumsHolder merge(GaussianNaiveBayesSumsHolder other) {
        featureSumsPerLbl = MapUtil.mergeMaps(featureSumsPerLbl, other.featureSumsPerLbl, this::sum, HashMap::new);
        featureSquaredSumsPerLbl = MapUtil.mergeMaps(featureSquaredSumsPerLbl, other.featureSquaredSumsPerLbl, this::sum, HashMap::new);
        featureCountersPerLbl = MapUtil.mergeMaps(featureCountersPerLbl, other.featureCountersPerLbl, (i1, i2) -> i1 + i2, HashMap::new);
        return this;
    }

    /** In-place operation. Sums {@code arr2} to {@code arr1} element to element. */
    private double[] sum(double[] arr1, double[] arr2) {
        for (int i = 0; i < arr1.length; i++)
            arr1[i] += arr2[i];

        return arr1;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
