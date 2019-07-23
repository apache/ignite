/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.selection.scoring.metric.regression;

import java.util.Iterator;
import org.apache.ignite.ml.selection.scoring.LabelPair;
import org.apache.ignite.ml.selection.scoring.metric.AbstractMetrics;

/**
 * Regression metrics calculator.
 * It could be used in two ways: to caculate all regression metrics or custom regression metric.
 */
public class RegressionMetrics extends AbstractMetrics<RegressionMetricValues> {
    /** Precision for R^2 calculation. */
    private static final double EPS = 0.00001;

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

        double sumOfLbls = 0.0;
        double sumOfSquaredLbls = 0.0;

        while (iter.hasNext()) {
            LabelPair<Double> e = iter.next();

            double prediction = e.getPrediction();
            double truth = e.getTruth();

            rss += Math.pow(prediction - truth, 2.0);
            mae += Math.abs(prediction - truth);

            totalAmount++;
            sumOfLbls += truth;
            sumOfSquaredLbls += Math.pow(truth, 2);
        }

        double meanOfLbls = sumOfLbls / totalAmount;
        double meanOfLblSquares = sumOfSquaredLbls / totalAmount;
        double tss = totalAmount * (meanOfLblSquares - Math.pow(meanOfLbls, 2));

        double r2 = 0.0;
        if (Math.abs(tss) < EPS) r2 = Math.abs(rss) < EPS ? 1.0 : 0.0;
        else r2 = 1 - rss / tss;

        mae /= totalAmount;

        return new RegressionMetricValues(totalAmount, rss, mae, r2);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "Regression metrics";
    }
}
