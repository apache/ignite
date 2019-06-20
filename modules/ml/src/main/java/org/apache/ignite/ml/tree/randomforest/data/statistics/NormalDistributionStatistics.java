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

import java.io.Serializable;

/**
 * Aggregator of normal distribution statistics for continual features.
 */
public class NormalDistributionStatistics implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -5422805289301484436L;

    /** Min value. */
    private final double min;

    /** Max value. */
    private final double max;

    /** Sum of value squares. */
    private final double sumOfSquares;

    /** Sum of values. */
    private final double sumOfValues;

    /** Count of objects. */
    private final long n;

    /**
     * Creates an instance of NormalDistributionStatistics.
     *
     * @param min Min.
     * @param max Max.
     * @param sumOfSquares Sum of squares.
     * @param sumOfValues Sum of values.
     * @param n N.
     */
    public NormalDistributionStatistics(double min, double max, double sumOfSquares, double sumOfValues, long n) {
        this.min = min;
        this.max = max;
        this.sumOfSquares = sumOfSquares;
        this.sumOfValues = sumOfValues;
        this.n = n;
    }

    /**
     * Returns plus of normal distribution statistics.
     *
     * @param stats Stats.
     * @return Plus of normal distribution statistics.
     */
    public NormalDistributionStatistics plus(NormalDistributionStatistics stats) {
        return new NormalDistributionStatistics(
            Math.min(this.min, stats.min),
            Math.max(this.max, stats.max),
            this.sumOfSquares + stats.sumOfSquares,
            this.sumOfValues + stats.sumOfValues,
            this.n + stats.n
        );
    }

    /**
     * @return Mean value.
     */
    public double mean() {
        return sumOfValues / n;
    }

    /**
     * @return Variance value.
     */
    public double variance() {
        double mean = mean();
        return (sumOfSquares / n) - mean * mean;
    }

    /**
     * @return Standard deviation value.
     */
    public double std() {
        return Math.sqrt(variance());
    }

    /**
     * @return Min value.
     */
    public double min() {
        return min;
    }

    /**
     * @return Max value.
     */
    public double max() {
        return max;
    }
}
