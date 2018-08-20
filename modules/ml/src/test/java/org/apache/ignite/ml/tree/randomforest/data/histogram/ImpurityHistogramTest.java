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

package org.apache.ignite.ml.tree.randomforest.data.histogram;

import java.util.Set;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;

import static org.junit.Assert.assertArrayEquals;

public class ImpurityHistogramTest {
    protected void checkBucketIds(Set<Integer> bucketIdsSet, Integer[] expected) {
        Integer[] bucketIds = new Integer[bucketIdsSet.size()];
        bucketIdsSet.toArray(bucketIds);
        assertArrayEquals(expected, bucketIds);
    }

    protected void checkCounters(FeatureHistogram<BaggedVector> hist, double[] expected) {
        double[] counters = hist.buckets().stream().mapToDouble(x -> hist.get(x).get()).toArray();
        assertArrayEquals(expected, counters, 0.01);
    }
}
