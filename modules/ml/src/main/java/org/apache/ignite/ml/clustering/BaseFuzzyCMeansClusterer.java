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

package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;

/** The abstract class that defines the basic interface of Fuzzy C-Means clusterers */
public abstract class BaseFuzzyCMeansClusterer<T extends Matrix> implements Clusterer<T, FuzzyCMeansModel> {
    /** Distance measure. */
    protected DistanceMeasure measure;

    /** Specific constant which is used in calculating of membership matrix. */
    protected double exponentialWeight;

    /** The maximum distance between old and new centers or the maximum difference between new and old membership matrix
     *  elements for which algorithm must stop. */
    protected double maxDelta;

    /** The flag that tells when algorithm should stop. */
    protected StopCondition stopCond;

    /**
     * Constructor that stores some required parameters.
     *
     * @param measure Distance measure.
     * @param exponentialWeight Specific constant which is used in calculating of membership matrix.
     * @param stopCond Flag that tells when algorithm should stop.
     * @param maxDelta The maximum distance between old and new centers or maximum difference between new and old
     *                 membership matrix elements for which algorithm must stop.
     */
    protected BaseFuzzyCMeansClusterer(DistanceMeasure measure, double exponentialWeight, StopCondition stopCond,
                                       double maxDelta) {
        this.measure = measure;
        this.exponentialWeight = exponentialWeight;
        this.stopCond = stopCond;
        this.maxDelta = maxDelta;
    }

    /**
     * Perform a cluster analysis on the given set of points.
     *
     * @param points The set of points.
     * @return A list of clusters.
     * @throws MathIllegalArgumentException If points are null or the number of data points is not compatible with this
     *                                      clusterer.
     * @throws ConvergenceException If the algorithm has not yet converged after the maximum number of iterations has
     *                              been exceeded.
     */
    public abstract FuzzyCMeansModel cluster(T points, int k);

    /**
     * Calculates the distance between two vectors. * with the configured {@link DistanceMeasure}.
     *
     * @return The distance between two points.
     */
    protected double distance(final Vector v1, final Vector v2) {
        return measure.compute(v1, v2);
    }

    /** Enumeration that contains different conditions under which algorithm must stop. */
    public enum StopCondition {
        /** Algorithm stops if the maximum distance between new and old centers is less than {@link #maxDelta}. */
        STABLE_CENTERS,

        /**
         * Algorithm stops if the maximum difference between elements of new and old membership matrix is less than
         * {@link #maxDelta}.
         */
        STABLE_MEMBERSHIPS
    }
}
