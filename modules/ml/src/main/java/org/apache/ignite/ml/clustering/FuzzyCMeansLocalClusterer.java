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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/** Implements the local version of Fuzzy C-Means algorithm for weighted points. */
public class FuzzyCMeansLocalClusterer extends BaseFuzzyCMeansClusterer<DenseLocalOnHeapMatrix> implements
        WeightedClusterer<DenseLocalOnHeapMatrix, FuzzyCMeansModel> {
    /** The maximum number of iterations. */
    private int maxIterations;

    /** The random numbers generator that is used to choose primary centers. */
    private Random rnd;

    /**
     * Constructor that retains all required parameters.
     *
     * @param measure Distance measure.
     * @param exponentialWeight Specific constant which is used in calculating of membership matrix.
     * @param stopCond Flag that tells when algorithm should stop.
     * @param maxDelta The maximum distance between old and new centers or maximum difference between new and old
     *                 membership matrix elements for which algorithm must stop.
     * @param maxIterations The maximum number of FCM iterations.
     */
    public FuzzyCMeansLocalClusterer(DistanceMeasure measure, double exponentialWeight, StopCondition stopCond,
                                     double maxDelta, int maxIterations, Long seed) {
        super(measure, exponentialWeight, stopCond, maxDelta);
        this.maxIterations = maxIterations;
        rnd = seed != null ? new Random(seed) : new Random();
    }

    /** {@inheritDoc} */
    @Override public FuzzyCMeansModel cluster(DenseLocalOnHeapMatrix points, int k) {
        List<Double> ones = new ArrayList<>(Collections.nCopies(points.rowSize(), 1.0));
        return cluster(points, k, ones);
    }

    /** {@inheritDoc} */
    @Override public FuzzyCMeansModel cluster(DenseLocalOnHeapMatrix points, int k, List<Double> weights)
            throws MathIllegalArgumentException, ConvergenceException {
        GridArgumentCheck.notNull(points, "points");
        GridArgumentCheck.notNull(weights, "weights");

        if (points.rowSize() != weights.size())
            throw new MathIllegalArgumentException("The number of points and the number of weights are not equal");

        if (k < 2)
            throw new MathIllegalArgumentException("The number of clusters is less than 2");

        Matrix centers = new DenseLocalOnHeapMatrix(k, points.columnSize());
        Matrix distances = new DenseLocalOnHeapMatrix(k, points.rowSize());
        Matrix membership = new DenseLocalOnHeapMatrix(k, points.rowSize());
        Vector weightsVector = new DenseLocalOnHeapVector(weights.size());
        for (int i = 0; i < weights.size(); i++)
            weightsVector.setX(i, weights.get(i));

        initializeCenters(centers, points, k, weightsVector);

        int iteration = 0;
        boolean finished = false;
        while (iteration < maxIterations && !finished) {
            calculateDistances(distances, points, centers);
            Matrix newMembership = calculateMembership(distances, weightsVector);
            Matrix newCenters = calculateNewCenters(points, newMembership);

            if (this.stopCond == StopCondition.STABLE_CENTERS)
                finished = areCentersStable(centers, newCenters);
            else
                finished = areMembershipStable(membership, newMembership);

            centers = newCenters;
            membership = newMembership;
            iteration++;
        }

        if (iteration == maxIterations)
            throw new ConvergenceException("Fuzzy C-Means algorithm has not converged after " +
                    Integer.toString(iteration) + " iterations");

        Vector[] centersArr = new Vector[k];
        for (int i = 0; i < k; i++)
            centersArr[i] = centers.getRow(i);

        return new FuzzyCMeansModel(centersArr, measure);
    }

    /**
     * Choose {@code k} centers according to their weights.
     *
     * @param centers Output matrix containing primary centers.
     * @param points Matrix of source points.
     * @param k The number of centers.
     * @param weights Vector of weights.
     */
    private void initializeCenters(Matrix centers, Matrix points, int k, Vector weights) {
        //int dimensions = points.columnSize();
        int numPoints = points.rowSize();

        Vector firstCenter = points.viewRow(rnd.nextInt(numPoints));
        centers.setRow(0, firstCenter.getStorage().data());

        Vector costs = points.foldRows(vector -> distance(vector, firstCenter));
        costs = costs.times(weights);

        double sum = costs.sum();

        for (int i = 1; i < k; i++) {
            double probe = rnd.nextDouble() * sum;
            double cntr = 0;
            int id = 0;

            for (int j = 0; j < numPoints; j++) {
                cntr += costs.getX(j);
                if (cntr >= probe) {
                    id = j;
                    break;
                }
            }

            centers.setRow(i, points.viewRow(id).getStorage().data());
            sum -= costs.get(id);
            costs.set(id, 0.0);
        }
    }

    /**
     * Calculate matrix of distances form each point to each center.
     *
     * @param distances Output matrix.
     * @param points Matrix that contains source points.
     * @param centers Matrix that contains centers.
     */
    private void calculateDistances(Matrix distances, Matrix points, Matrix centers) {
        int numPoints = points.rowSize();
        int numCenters = centers.rowSize();

        for (int i = 0; i < numCenters; i++)
            for (int j = 0; j < numPoints; j++)
                distances.set(i, j, distance(centers.viewRow(i), points.viewRow(j)));
    }

    /**
     * Calculate membership matrix.
     *
     * @param distances Matrix of distances.
     * @param weights Vector of weights.
     * @
     */
    private Matrix calculateMembership(Matrix distances, Vector weights) {
        Matrix newMembership = new DenseLocalOnHeapMatrix(distances.rowSize(), distances.columnSize());
        int numPoints = distances.columnSize();
        int numCenters = distances.rowSize();
        double fuzzyMembershipCoefficient = 2 / (exponentialWeight - 1);

        for (int i = 0; i < numCenters; i++) {
            for (int j = 0; j < numPoints; j++) {
                double invertedFuzzyWeight = 0.0;

                for (int k = 0; k < numCenters; k++) {
                    double val = Math.pow(distances.get(i, j) / distances.get(k, j),
                            fuzzyMembershipCoefficient);
                    if (Double.isNaN(val))
                        val = 1.0;

                    invertedFuzzyWeight += val;
                }

                double weight = 1.0 / invertedFuzzyWeight * weights.getX(j);
                newMembership.setX(i, j, Math.pow(weight, exponentialWeight));
            }
        }
        return newMembership;
    }

    /**
     * Calculate new centers using membership matrix.
     *
     * @param points Matrix of source points.
     * @param membership Matrix that contains membership coefficients.
     * @return Matrix that contains new centers.
     */
    private Matrix calculateNewCenters(Matrix points, Matrix membership) {
        Vector membershipSums = membership.foldRows(Vector::sum);
        Matrix newCenters = membership.times(points);

        int numCenters = newCenters.rowSize();
        for (int i = 0; i < numCenters; i++)
            newCenters.viewRow(i).divide(membershipSums.getX(i));

        return newCenters;
    }

    /**
     * Check if centers have moved insignificantly.
     *
     * @param centers Old centers.
     * @param newCenters New centers.
     * @return The result of comparison.
     */
    private boolean areCentersStable(Matrix centers, Matrix newCenters) {
        int numCenters = centers.rowSize();
        for (int i = 0; i < numCenters; i++)
            if (distance(centers.viewRow(i), newCenters.viewRow(i)) > maxDelta)
                return false;

        return true;
    }

    /**
     * Check if membership matrix has changed insignificantly.
     *
     * @param membership Old membership matrix.
     * @param newMembership New membership matrix.
     * @return The result of comparison.
     */
    private boolean areMembershipStable(Matrix membership, Matrix newMembership) {
        int numCenters = membership.rowSize();
        int numPoints = membership.columnSize();

        for (int i = 0; i < numCenters; i++)
            for (int j = 0; j < numPoints; j++)
                if (Math.abs(newMembership.getX(i, j) - membership.getX(i, j)) > maxDelta)
                    return false;

        return true;
    }
}
