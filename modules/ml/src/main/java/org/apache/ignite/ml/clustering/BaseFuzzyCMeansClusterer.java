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

import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;

public abstract class BaseFuzzyCMeansClusterer<T extends Matrix> implements Clusterer<T, FuzzyCMeansModel> {

    /** distance measure */
    protected DistanceMeasure measure;

    /** specific constant which is used in calculating of membership matrix */
    protected double exponentialWeight;

    /** max distance between old and new centers which indicates when algorithm should stop */
    protected double maxCentersDelta;

    /**
     * Constructor that stores some required parameters
     *
     * @param measure distance measure
     * @param exponentialWeight specific constant which is used in calculating of membership matrix
     * @param maxCentersDelta max distance between old and new centers which indicates when algorithm should stop
     */
    protected BaseFuzzyCMeansClusterer(DistanceMeasure measure, double exponentialWeight, double maxCentersDelta) {
        this.measure = measure;
        this.exponentialWeight = exponentialWeight;
        this.maxCentersDelta = maxCentersDelta;
    }

    /**
     * Perform a cluster analysis on the given set of points.
     *
     * @param points the set of points
     * @return a list of clusters
     * @throws MathIllegalArgumentException if points are null or the number of data points is not compatible with this
     * clusterer
     * @throws ConvergenceException if the algorithm has not yet converged after the maximum number of iterations has
     * been exceeded
     */
    public abstract FuzzyCMeansModel cluster(T points, int k);

    /**
     * Calculates the distance between two vectors. * with the configured {@link DistanceMeasure}.
     *
     * @return the distance between the two points
     */
    protected double distance(final Vector v1, final Vector v2) {
        return measure.compute(v1, v2);
    }
}
