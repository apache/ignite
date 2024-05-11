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

import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;

/**
 * Implementation of {@link LeafValuesComputer} for regression task.
 */
public class RegressionLeafValuesComputer extends LeafValuesComputer<MeanValueStatistic> {
    /** Serial version uid. */
    private static final long serialVersionUID = -1898031675220962125L;

    /** {@inheritDoc} */
    @Override protected void addElementToLeafStatistic(MeanValueStatistic leafStatAggr,
        BootstrappedVector vec, int sampleId) {

        int numOfRepetitions = vec.counters()[sampleId];
        leafStatAggr.setSumOfValues(leafStatAggr.getSumOfValues() + vec.label() * numOfRepetitions);
        leafStatAggr.setCntOfValues(leafStatAggr.getCntOfValues() + numOfRepetitions);
    }

    /** {@inheritDoc} */
    @Override protected MeanValueStatistic mergeLeafStats(MeanValueStatistic leftStats,
        MeanValueStatistic rightStats) {

        return new MeanValueStatistic(
            leftStats.getSumOfValues() + rightStats.getSumOfValues(),
            leftStats.getCntOfValues() + rightStats.getCntOfValues()
        );
    }

    /** {@inheritDoc} */
    @Override protected MeanValueStatistic createLeafStatsAggregator(int sampleId) {
        return new MeanValueStatistic(0.0, 0);
    }

    /**
     * Returns the mean value in according to statistic.
     *
     * @param stat Leaf statistics.
     */
    @Override protected double computeLeafValue(MeanValueStatistic stat) {
        return stat.mean();
    }

}
