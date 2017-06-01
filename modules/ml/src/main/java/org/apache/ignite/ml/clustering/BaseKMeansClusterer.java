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

import java.util.List;

import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * This class is partly based on the corresponding class from Apache Common Math lib.
 */
public abstract class BaseKMeansClusterer<T extends Matrix> implements Clusterer<T, KMeansModel> {
    /** The distance measure to use. */
    private DistanceMeasure measure;

    /**
     * Build a new clusterer with the given {@link DistanceMeasure}.
     *
     * @param measure the distance measure to use
     */
    protected BaseKMeansClusterer(final DistanceMeasure measure) {
        this.measure = measure;
    }

    /**
     * Perform a cluster analysis on the given set of points.
     *
     * @param points the set of points
     * @return a {@link List} of clusters
     * @throws MathIllegalArgumentException if points are null or the number of data points is not compatible with this
     * clusterer
     * @throws ConvergenceException if the algorithm has not yet converged after the maximum number of iterations has
     * been exceeded
     */
    public abstract KMeansModel cluster(T points, int k)
        throws MathIllegalArgumentException, ConvergenceException;

    /**
     * Returns the {@link DistanceMeasure} instance used by this clusterer.
     *
     * @return the distance measure
     */
    public DistanceMeasure getDistanceMeasure() {
        return measure;
    }

    /**
     * Calculates the distance between two vectors.
     * with the configured {@link DistanceMeasure}.
     *
     * @return the distance between the two clusterables
     */
    protected double distance(final Vector v1, final Vector v2) {
        return measure.compute(v1, v2);
    }

    /**
     * Find the closest cluster center index and distance to it from a given point.
     *
     * @param centers Centers to look in.
     * @param pnt Point.
     */
    protected IgniteBiTuple<Integer, Double> findClosest(Vector[] centers, Vector pnt) {
        double bestDistance = Double.POSITIVE_INFINITY;
        int bestInd = 0;

        for (int i = 0; i < centers.length; i++) {
            double dist = distance(centers[i], pnt);
            if (dist < bestDistance) {
                bestDistance = dist;
                bestInd = i;
            }
        }

        return new IgniteBiTuple<>(bestInd, bestDistance);
    }
}
