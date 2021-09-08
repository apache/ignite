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
 * F-measure metric class.
 */
public class FMeasure<L extends Serializable> extends BinaryClassificationMetric<L> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 9156779288165194348L;

    /**
     * Square of beta parameter for F-score.
     */
    private final double betaSquare;

    /**
     * Precision.
     */
    private final Precision<L> precision = new Precision<>();

    /**
     * Recall.
     */
    private final Recall<L> recall = new Recall<>();

    /**
     * Fscore.
     */
    private Double fscore = Double.NaN;

    /**
     * Creates an instance of FScore class.
     *
     * @param beta Beta (see https://en.wikipedia.org/wiki/F1_score ).
     */
    public FMeasure(double beta) {
        betaSquare = Math.pow(beta, 2);
    }

    /**
     * Creates an instance of FScore class.
     */
    public FMeasure() {
        betaSquare = 1;
    }

    /**
     * Creates an instance of FScore class.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label.
     * @param betaSquare Squared beta parameter.
     */
    public FMeasure(L truthLabel, L falseLabel, double betaSquare) {
        super(truthLabel, falseLabel);
        this.betaSquare = betaSquare;
    }

    /**
     * Creates an instance of FScore class.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label.
     */
    public FMeasure(L truthLabel, L falseLabel) {
        super(truthLabel, falseLabel);
        this.betaSquare = 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override public FMeasure<L> initBy(BinaryClassificationPointwiseMetricStatsAggregator<L> aggr) {
        precision.initBy(aggr);
        recall.initBy(aggr);

        double nom = (1 + betaSquare) * precision.value() * recall.value();
        double denom = (betaSquare * precision.value() + recall.value());
        fscore = nom / denom;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double value() {
        return fscore;
    }

    /**
     * {@inheritDoc}
     */
    @Override public MetricName name() {
        return MetricName.F_MEASURE;
    }
}
