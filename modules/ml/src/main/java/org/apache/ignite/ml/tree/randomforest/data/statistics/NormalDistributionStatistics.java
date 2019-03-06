/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
     * @return plus of normal distribution statistics.
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
     * @return mean value.
     */
    public double mean() {
        return sumOfValues / n;
    }

    /**
     * @return variance value.
     */
    public double variance() {
        double mean = mean();
        return (sumOfSquares / n) - mean * mean;
    }

    /**
     * @return standard deviation value.
     */
    public double std() {
        return Math.sqrt(variance());
    }

    /**
     * @return min value.
     */
    public double min() {
        return min;
    }

    /**
     * @return max value.
     */
    public double max() {
        return max;
    }
}
