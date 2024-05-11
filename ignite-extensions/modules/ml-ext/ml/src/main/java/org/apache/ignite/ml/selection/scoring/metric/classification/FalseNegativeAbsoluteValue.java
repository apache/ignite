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
 * FalseNegative metric class.
 */
public class FalseNegativeAbsoluteValue<L extends Serializable> extends BinaryClassificationMetric<L> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 1888543464986349144L;

    /**
     * FN value.
     */
    private Double fn = Double.NaN;

    /**
     * Creates an instance of FalseNegativeAbsoluteValue.
     */
    public FalseNegativeAbsoluteValue() {
    }

    /**
     * Creates an instance of FalseNegativeAbsoluteValue class.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label.
     */
    public FalseNegativeAbsoluteValue(L truthLabel, L falseLabel) {
        super(truthLabel, falseLabel);
    }

    /**
     * {@inheritDoc}
     */
    @Override public FalseNegativeAbsoluteValue<L> initBy(BinaryClassificationPointwiseMetricStatsAggregator<L> aggr) {
        fn = (double)aggr.getFalseNegative();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double value() {
        return fn;
    }

    /**
     * {@inheritDoc}
     */
    @Override public MetricName name() {
        return MetricName.FALSE_NEGATIVE;
    }
}
