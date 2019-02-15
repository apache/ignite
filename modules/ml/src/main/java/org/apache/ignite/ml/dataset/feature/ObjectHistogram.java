/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.dataset.feature;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Basic implementation of {@link Histogram} that implements also {@link DistributionComputer}.
 *
 * @param <T> Type of object for histogram.
 */
public abstract class ObjectHistogram<T> implements Histogram<T, ObjectHistogram<T>>, DistributionComputer {
    /** Serial version uid. */
    private static final long serialVersionUID = -2708731174031404487L;

    /** Histogram. */
    private final TreeMap<Integer, Double> hist = new TreeMap<>();

    /** {@inheritDoc} */
    @Override public void addElement(T val) {
        Integer bucket = mapToBucket(val);
        Double cntrVal = mapToCounter(val);

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
        ObjectHistogram<T> res = newInstance();
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
    @Override public boolean isEqualTo(ObjectHistogram<T> other) {
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

    /**
     * Bucket mapping.
     *
     * @param obj Object.
     * @return BucketId.
     */
    public abstract Integer mapToBucket(T obj);

    /**
     * Counter mapping.
     *
     * @param obj Object.
     * @return counter.
     */
    public abstract Double mapToCounter(T obj);

    /**
     * Creates an instance of ObjectHistogram from child class.
     *
     * @return object histogram.
     */
    public abstract ObjectHistogram<T> newInstance();
}
