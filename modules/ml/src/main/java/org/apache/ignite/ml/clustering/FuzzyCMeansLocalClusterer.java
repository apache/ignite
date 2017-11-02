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

import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** Implements the local version of Fuzzy C-Means algorithm for weighted data */
public class FuzzyCMeansLocalClusterer extends BaseFuzzyCMeansClusterer<DenseLocalOnHeapMatrix> implements
        WeightedClusterer<DenseLocalOnHeapMatrix, FuzzyCMeansModel> {

    /** maximum number of iterations */
    private int maxIterations;

    /** the random numbers generator used to choose primary centers */
    private Random rand;

    /**
     * Constructor that stores all required parameters
     *
     * @param measure distance measure
     * @param exponentialWeight specific constant which is used in calculating of membership matrix
     * @param maxCentersDelta max distance between old and new centers which indicates when algorithm should stop
     * @param maxIterations the maximum number of iterations
     * @param seed seed for random numbers generator
     */
    public FuzzyCMeansLocalClusterer(DistanceMeasure measure, double exponentialWeight,
                                     double maxCentersDelta, int maxIterations, Long seed) {
        super(measure, exponentialWeight, maxCentersDelta);
        this.maxIterations = maxIterations;
        rand = seed != null ? new Random(seed) : new Random();
    }

    /** {@inheritDoc} */
    @Override
    public FuzzyCMeansModel cluster(DenseLocalOnHeapMatrix points, int k) {
        List<Double> ones = new ArrayList<>(Collections.nCopies(points.rowSize(), 1.0));
        return cluster(points, k, ones);
    }

    /** {@inheritDoc} */
    @Override
    public FuzzyCMeansModel cluster(DenseLocalOnHeapMatrix points, int k, List<Double> weights)
            throws MathIllegalArgumentException, ConvergenceException {
        GridArgumentCheck.notNull(points, "points");
        GridArgumentCheck.notNull(weights, "weights");

        if (points.rowSize() != weights.size()) {
            throw new MathIllegalArgumentException("The number of points and the number of weights are not equal");
        }

        if (k < 2) {
            throw new MathIllegalArgumentException("The number of clusters is less than 2");
        }

        Matrix centers = new DenseLocalOnHeapMatrix(k, points.columnSize());
        Matrix distances = new DenseLocalOnHeapMatrix(k, points.rowSize());
        Matrix membership = new DenseLocalOnHeapMatrix(k, points.rowSize());
        Vector weightsVector = new DenseLocalOnHeapVector(weights.size());
        for (int i = 0; i < weights.size(); i++) {
            weightsVector.setX(i, weights.get(i));
        }

        initializeCenters(centers, points, k, weightsVector);

        int iteration = 0;
        boolean finished = false;
        while (iteration < maxIterations && !finished) {
            calculateDistances(distances, points, centers);
            Matrix newMembership = calculateMembership(distances, weightsVector);
            Matrix newCenters = calculateNewCenters(points, newMembership);

            finished = isFinished(centers, newCenters, membership, newMembership);
            centers = newCenters;
            membership = newMembership;
            iteration++;
        }

        if (iteration == maxIterations) {
            throw new ConvergenceException("Fuzzy C-Means algorithm has not converged after " +
                    Integer.toString(iteration) + " iterations");
        }

        Vector[] centersArray = new Vector[k];
        for (int i = 0; i < k; i++) {
            centersArray[i] = centers.getRow(i);
        }

        return new FuzzyCMeansModel(centersArray, measure);
    }

    /**
     * Choose k centers according to their weights
     *
     * @param centers output matrix
     * @param points matrix of source points
     * @param k number of centers
     * @param weights weights vector
     */
    private void initializeCenters(Matrix centers, Matrix points, int k, Vector weights) {
        //int dimensions = points.columnSize();
        int numPoints = points.rowSize();

        Vector firstCenter = points.viewRow(rand.nextInt(numPoints));
        centers.setRow(0, firstCenter.getStorage().data());

        Vector costs = points.foldRows(vector -> distance(vector, firstCenter));
        costs = costs.times(weights);

        double sum = costs.sum();

        for (int i = 1; i < k; i++) {
            double probe = rand.nextDouble() * sum;
            double counter = 0;
            int id = 0;

            for (int j = 0; j < numPoints; j++) {
                counter += costs.getX(j);
                if (counter >= probe) {
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
     * Calculate matrix of distances form each point to each center
     *
     * @param distances output matrix
     * @param points matrix of source points
     * @param centers matrix of centers
     */
    private void calculateDistances(Matrix distances, Matrix points, Matrix centers) {
        int numPoints = points.rowSize();
        int numCenters = centers.rowSize();

        for (int i = 0; i < numCenters; i++) {
            for (int j = 0; j < numPoints; j++) {
                distances.set(i, j, distance(centers.viewRow(i), points.viewRow(j)));
            }
        }
    }

    /**
     * Calculate membership matrix
     *
     * @param distances matrix of distances
     * @param weights vector of weights
     * @
     */
    private Matrix calculateMembership(Matrix distances, Vector weights) {
        Matrix newMembership = new DenseLocalOnHeapMatrix(distances.rowSize(), distances.columnSize());
        int numPoints = distances.columnSize();
        int numCenters = distances.rowSize();
        double fuzzyMembershipCoefficient = 2 / (exponentialWeight - 1);

        for (int i = 0; i < numCenters; i++) {
            for (int j = 0; j < numPoints; j++) {
                double invertedFuzzyWeight = 0;
                for (int k = 0; k < numCenters; k++) {
                    double value = Math.pow(distances.get(i, j) / distances.get(k, j),
                            fuzzyMembershipCoefficient);
                    if (Double.isNaN(value)) {
                        value = 1.0;
                    }
                    invertedFuzzyWeight += value;
                }
                double weight = 1.0 / invertedFuzzyWeight * weights.getX(j);
                newMembership.setX(i, j, Math.pow(weight, exponentialWeight));
            }
        }
        return newMembership;
    }

    /**
     * Calculate new centers using membership matrix
     *
     * @param points matrix of source points
     * @param membership membership matrix`
     * @return matrix of centers
     */
    private Matrix calculateNewCenters(Matrix points, Matrix membership) {
        Vector membershipSums = membership.foldRows(row -> row.sum());
        Matrix newCenters = membership.times(points);

        int numCenters = newCenters.rowSize();
        for (int i = 0; i < numCenters; i++) {
            newCenters.viewRow(i).divide(membershipSums.getX(i));
        }

        return newCenters;
    }

    /**
     * Check if centers have moved insignificantly
     *
     * @param centers old centers
     * @param newCenters new centers
     * @return the result of comparison
     */
    private boolean isFinished(Matrix centers, Matrix newCenters, Matrix membership, Matrix newMembership) {
        int numCenters = centers.rowSize();
        int numPoints = membership.columnSize();

        for (int i = 0; i < numCenters; i++) {
            if (distance(centers.viewRow(i), newCenters.viewRow(i)) > maxCentersDelta) {
                return false;
            }

            //TODO: stop condition flag

           for (int j = 0; j < numPoints; j++) {
                if (Math.abs(newMembership.getX(i, j) - membership.getX(i, j)) > maxCentersDelta / numCenters) {
                    return false;
                }
            }
        }
        return true;
    }
}
