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

package org.apache.ignite.ml.tree.randomforest.data.statistics;

import java.util.Comparator;
import java.util.Map;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;

/**
 * Implementation of {@link LeafValuesComputer} for classification task.
 */
public class ClassifierLeafValuesComputer extends LeafValuesComputer<ObjectHistogram<BootstrappedVector>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 420416095877577599L;

    /** Label mapping. */
    private final Map<Double, Integer> lblMapping;

    /**
     * Creates an instance of ClassifierLeafValuesComputer.
     *
     * @param lblMapping Label mapping.
     */
    public ClassifierLeafValuesComputer(Map<Double, Integer> lblMapping) {
        this.lblMapping = lblMapping;
    }

    /** {@inheritDoc} */
    @Override protected void addElementToLeafStatistic(ObjectHistogram<BootstrappedVector> leafStatAggr, BootstrappedVector vec, int sampleId) {
        leafStatAggr.addElement(vec);
    }

    /** {@inheritDoc} */
    @Override protected ObjectHistogram<BootstrappedVector> mergeLeafStats(ObjectHistogram<BootstrappedVector> leftStats,
        ObjectHistogram<BootstrappedVector> rightStats) {

        return leftStats.plus(rightStats);
    }

    /** {@inheritDoc} */
    @Override protected ObjectHistogram<BootstrappedVector> createLeafStatsAggregator(int sampleId) {
        return new ObjectHistogram<>(
            x -> lblMapping.get(x.label()),
            x -> (double)x.counters()[sampleId]
        );
    }

    /**
     * Returns the most frequent value in according to statistic.
     *
     * @param stat Leaf statistics.
     */
    @Override protected double computeLeafValue(ObjectHistogram<BootstrappedVector> stat) {
        Integer bucketId = stat.buckets().stream()
            .max(Comparator.comparing(b -> stat.getValue(b).orElse(0.0)))
            .orElse(-1);

        if(bucketId == -1)
            return Double.NaN;

        return lblMapping.entrySet().stream()
            .filter(x -> x.getValue().equals(bucketId))
            .findFirst()
            .get().getKey();
    }
}
