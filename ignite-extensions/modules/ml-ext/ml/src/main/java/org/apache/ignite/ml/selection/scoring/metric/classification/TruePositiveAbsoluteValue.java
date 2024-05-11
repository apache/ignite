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
 * TruePositive metric class.
 */
public class TruePositiveAbsoluteValue<L extends Serializable> extends BinaryClassificationMetric<L> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 5126895790009271001L;

    /**
     * TP value.
     */
    private Double tp = Double.NaN;

    /**
     * Creates an instance of TruePositiveAbsoluteValue class.
     */
    public TruePositiveAbsoluteValue() {
    }

    /**
     * Creates an instance of TruePositiveAbsoluteValue class.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label.
     */
    public TruePositiveAbsoluteValue(L truthLabel, L falseLabel) {
        super(truthLabel, falseLabel);
    }

    /**
     * {@inheritDoc}
     */
    @Override public TruePositiveAbsoluteValue<L> initBy(BinaryClassificationPointwiseMetricStatsAggregator<L> aggr) {
        tp = (double)aggr.getTruePositive();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double value() {
        return tp;
    }

    /**
     * {@inheritDoc}
     */
    @Override public MetricName name() {
        return MetricName.TRUE_POSITIVE;
    }
}
