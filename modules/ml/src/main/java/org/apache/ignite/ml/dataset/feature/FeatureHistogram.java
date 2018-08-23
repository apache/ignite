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

package org.apache.ignite.ml.dataset.feature;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class FeatureHistogram<T> implements Histogram<T, FeatureHistogram<T>>, DistributionComputer {
    private final IgniteFunction<T, Integer> bucketMapping;
    private final IgniteFunction<T, Double> mappingToCounter;
    private final Map<Integer, Double> hist;

    public FeatureHistogram(IgniteFunction<T, Integer> bucketMapping,
        IgniteFunction<T, Double> mappingToCounter) {

        this.bucketMapping = bucketMapping;
        this.mappingToCounter = mappingToCounter;
        this.hist = new TreeMap<>(Integer::compareTo);
    }

    @Override public void addElement(T value) {
        Integer bucket = bucketMapping.apply(value);
        Double counterValue = mappingToCounter.apply(value);

        assert counterValue >= 0;
        Double bucketValue = hist.getOrDefault(bucket, 0.0);
        hist.put(bucket, bucketValue + counterValue);
    }

    @Override public void addHist(FeatureHistogram<T> other) {
        other.hist.forEach((bucket, counter) -> {
            Double bucketValue = hist.getOrDefault(bucket, 0.0);
            hist.put(bucket, bucketValue + counter);
        });
    }

    @Override public Set<Integer> buckets() {
        return hist.keySet();
    }

    @Override public Optional<Double> get(Integer bucket) {
        return Optional.ofNullable(hist.get(bucket));
    }

    @Override public TreeMap<Integer, Double> computeDistributionFunction() {
        TreeMap<Integer, Double> result = new TreeMap<>();

        double accum = 0.0;
        for (Integer bucket : hist.keySet()) {
            accum += hist.get(bucket);
            result.put(bucket, accum);
        }

        return result;
    }

}
