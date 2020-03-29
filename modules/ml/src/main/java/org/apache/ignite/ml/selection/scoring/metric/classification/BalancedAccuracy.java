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

package org.apache.ignite.ml.selection.scoring.metric.classification;

import java.io.Serializable;
import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.BinaryClassificationPointwiseMetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;

/**
 * BalancedAccuracy metric class.
 */
public class BalancedAccuracy<L extends Serializable> extends BinaryClassificationMetric<L> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 91677523635381939L;

    /**
     * Metric value.
     */
    private Double val = Double.NaN;

    /**
     * Creates an instance of BalancedAccuracy.
     */
    public BalancedAccuracy() {
    }

    /**
     * Creates an instance of BalancedAccuracy class.
     *
     * @param truthLb Truth label.
     * @param falseLb False label.
     */
    public BalancedAccuracy(L truthLb, L falseLb) {
        super(truthLb, falseLb);
    }

    /**
     * {@inheritDoc}
     */
    @Override public BalancedAccuracy<L> initBy(BinaryClassificationPointwiseMetricStatsAggregator<L> aggr) {
        int n = aggr.getTrueNegative() + aggr.getFalsePositive();
        int p = aggr.getTruePositive() + aggr.getFalseNegative();
        val = n == 0 && p == 0 ? 1 : ((double)aggr.getTruePositive() / p + (double)aggr.getTrueNegative() / n) / 2;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double value() {
        return val;
    }

    /**
     * {@inheritDoc}
     */
    @Override public MetricName name() {
        return MetricName.BALANCED_ACCURACY;
    }
}
