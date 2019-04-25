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
package org.apache.ignite.ml.naivebayes.discrete;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.util.MapUtil;

/** Service class is used to calculate amount of values which are below the threshold. */
public class DiscreteNaiveBayesSumsHolder implements AutoCloseable, Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -2059362365851744206L;
    /** Sums of values correspones to a particular bucket for all features for each label */
    Map<Double, long[][]> valuesInBucketPerLbl = new HashMap<>();
    /** Rows count for each label */
    Map<Double, Integer> featureCountersPerLbl = new HashMap<>();

    /** Merge to current */
    DiscreteNaiveBayesSumsHolder merge(DiscreteNaiveBayesSumsHolder other) {
        valuesInBucketPerLbl = MapUtil.mergeMaps(valuesInBucketPerLbl, other.valuesInBucketPerLbl, this::sum, HashMap::new);
        featureCountersPerLbl = MapUtil.mergeMaps(featureCountersPerLbl, other.featureCountersPerLbl, (i1, i2) -> i1 + i2, HashMap::new);
        return this;
    }

    /** In-place operation. Sums {@code arr2} to {@code arr1} element to element. */
    private long[][] sum(long[][] arr1, long[][] arr2) {
        for (int i = 0; i < arr1.length; i++) {
            for (int j = 0; j < arr1[i].length; j++)
                arr1[i][j] += arr2[i][j];
        }

        return arr1;
    }

    /** */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
