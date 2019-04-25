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

import org.apache.ignite.ml.selection.scoring.metric.MetricValues;

/**
 * Provides access to regression metric values.
 */
public class RegressionMetricValues implements MetricValues {
    /** Mean absolute error. */
    private double mae;

    /** Mean squared error. */
    private double mse;

    /** Residual sum of squares. */
    private double rss;

    /** Root mean squared error. */
    private double rmse;

    /** Coefficient of determination. */
    private double r2;

    /**
     * Initalize an instance.
     *
     * @param totalAmount Total amount of observations.
     * @param rss         Residual sum of squares.
     * @param mae         Mean absolute error.
     * @param r2          Coefficient of determintaion.
     */
    public RegressionMetricValues(int totalAmount, double rss, double mae, double r2) {
        this.rss = rss;
        this.mse = rss / totalAmount;
        this.rmse = Math.sqrt(this.mse);
        this.mae = mae;
        this.r2 = r2;
    }

    /** Returns mean absolute error. */
    public double mae() {
        return mae;
    }

    /** Returns mean squared error. */
    public double mse() {
        return mse;
    }

    /** Returns residual sum of squares. */
    public double rss() {
        return rss;
    }

    /** Returns root mean squared error. */
    public double rmse() {
        return rmse;
    }

    /** Returns coefficient of determination. */
    public double r2() {
        return r2;
    }
}
