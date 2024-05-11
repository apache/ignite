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
 * Precision metric class for binary classification.
 */
public class Precision<L extends Serializable> extends BinaryClassificationMetric<L> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 2112795951652050170L;

    /**
     * Precision.
     */
    private Double precision = Double.NaN;

    /**
     * Creates an instance Precision class.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label.
     */
    public Precision(L truthLabel, L falseLabel) {
        super(truthLabel, falseLabel);
    }

    /**
     * Creates an instance Precision class.
     */
    public Precision() {
    }

    /**
     * {@inheritDoc}
     */
    @Override public Precision<L> initBy(BinaryClassificationPointwiseMetricStatsAggregator<L> aggr) {
        precision = ((double)(aggr.getTruePositive()) / (aggr.getTruePositive() + aggr.getFalsePositive()));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double value() {
        return precision;
    }

    /**
     * {@inheritDoc}
     */
    @Override public MetricName name() {
        return MetricName.PRECISION;
    }
}
