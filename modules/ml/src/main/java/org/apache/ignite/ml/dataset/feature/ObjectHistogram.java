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

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Basic implementation of {@link Histogram} that implements also {@link DistributionComputer}.
 *
 * @param <T> Type of object for histogram.
 */
public class ObjectHistogram<T> implements Histogram<T, ObjectHistogram<T>>, DistributionComputer {
    /** Serial version uid. */
    private static final long serialVersionUID = -2708731174031404487L;

    /** Bucket mapping. */
    private final IgniteFunction<T, Integer> bucketMapping;

    /** Mapping to counter. */
    private final IgniteFunction<T, Double> mappingToCntr;

    /** Histogram. */
    private final Map<Integer, Double> hist;

    /**
     * Create an instance of ObjectHistogram.
     *
     * @param bucketMapping Bucket mapping.
     * @param mappingToCntr Mapping to counter.
     */
    public ObjectHistogram(IgniteFunction<T, Integer> bucketMapping,
        IgniteFunction<T, Double> mappingToCntr) {

        this.bucketMapping = bucketMapping;
        this.mappingToCntr = mappingToCntr;
        this.hist = new TreeMap<>(Integer::compareTo);
    }

    /** {@inheritDoc} */
    @Override public void addElement(T val) {
        Integer bucket = bucketMapping.apply(val);
        Double cntrVal = mappingToCntr.apply(val);

        assert cntrVal >= 0;
        Double bucketVal = hist.getOrDefault(bucket, 0.0);
        hist.put(bucket, bucketVal + cntrVal);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> buckets() {
        return hist.keySet();
    }

    /** {@inheritDoc} */
    @Override public Optional<Double> getValue(Integer bucketId) {
        return Optional.ofNullable(hist.get(bucketId));
    }

    /** {@inheritDoc} */
    @Override public TreeMap<Integer, Double> computeDistributionFunction() {
        TreeMap<Integer, Double> res = new TreeMap<>();

        double accum = 0.0;
        for (Integer bucket : hist.keySet()) {
            accum += hist.get(bucket);
            res.put(bucket, accum);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public ObjectHistogram<T> plus(ObjectHistogram<T> other) {
        ObjectHistogram<T> res = new ObjectHistogram<>(bucketMapping, mappingToCntr);
        addTo(this.hist, res.hist);
        addTo(other.hist, res.hist);
        return res;
    }

    /**
     * Adds bucket values to target histogram.
     *
     * @param from From.
     * @param to To.
     */
    private void addTo(Map<Integer, Double> from, Map<Integer, Double> to) {
        from.forEach((bucket, value) -> {
            Double putVal = to.getOrDefault(bucket, 0.0);
            to.put(bucket, putVal + value);
        });
    }

    /** {@inheritDoc} */
    public boolean isEqualTo(ObjectHistogram<T> other) {
        Set<Integer> totalBuckets = new HashSet<>(buckets());
        totalBuckets.addAll(other.buckets());
        if(totalBuckets.size() != buckets().size())
            return false;

        for(Integer bucketId : totalBuckets) {
            double leftVal = hist.get(bucketId);
            double rightVal = other.hist.get(bucketId);
            if(Math.abs(leftVal - rightVal) > 0.001)
                return false;
        }

        return true;
    }
}
