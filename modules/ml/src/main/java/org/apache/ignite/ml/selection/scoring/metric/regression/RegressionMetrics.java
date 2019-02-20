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

package org.apache.ignite.ml.selection.scoring.metric.regression;

import org.apache.ignite.ml.selection.scoring.LabelPair;
import org.apache.ignite.ml.selection.scoring.metric.AbstractMetrics;

import java.util.Iterator;

/**
 * Regression metrics calculator.
 * It could be used in two ways: to caculate all regression metrics or custom regression metric.
 */
public class RegressionMetrics extends AbstractMetrics<RegressionMetricValues> {
    {
        metric = RegressionMetricValues::rmse;
    }

    /**
     * Calculates regression metrics values.
     *
     * @param iter Iterator that supplies pairs of truth values and predicated.
     * @return Scores for all regression metrics.
     */
    @Override public RegressionMetricValues scoreAll(Iterator<LabelPair<Double>> iter) {
        int totalAmount = 0;
        double rss = 0.0;
        double mae = 0.0;

        while (iter.hasNext()) {
            LabelPair<Double> e = iter.next();

            double prediction = e.getPrediction();
            double truth = e.getTruth();

            rss += Math.pow(prediction - truth, 2.0);
            mae += Math.abs(prediction - truth);

            totalAmount++;
        }

        mae /= totalAmount;

        return new RegressionMetricValues(totalAmount, rss, mae);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "Regression metrics";
    }
}
