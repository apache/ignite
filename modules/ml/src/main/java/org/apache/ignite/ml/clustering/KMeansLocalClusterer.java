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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.ml.math.*;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

import static org.apache.ignite.ml.math.util.MatrixUtil.localCopyOf;

/**
 * Perform clusterization on local data.
 * This class is based on Apache Spark class with corresponding functionality.
 */
public class KMeansLocalClusterer extends BaseKMeansClusterer<DenseLocalOnHeapMatrix> implements
    WeightedClusterer<DenseLocalOnHeapMatrix, KMeansModel> {
    /** */
    private int maxIterations;

    /** */
    private Random rand;

    /**
     * Build a new clusterer with the given {@link DistanceMeasure}.
     *
     * @param measure Distance measure to use.
     * @param maxIterations maximal number of iterations.
     * @param seed Seed used in random parts of algorithm.
     */
    public KMeansLocalClusterer(DistanceMeasure measure, int maxIterations, Long seed) {
        super(measure);
        this.maxIterations = maxIterations;
        rand = seed != null ? new Random(seed) : new Random();
    }

    /** {@inheritDoc} */
    @Override public KMeansModel cluster(
        DenseLocalOnHeapMatrix points, int k) throws MathIllegalArgumentException, ConvergenceException {
        List<Double> ones = new ArrayList<>(Collections.nCopies(points.rowSize(), 1.0));
        return cluster(points, k, ones);
    }

    /** {@inheritDoc} */
    @Override public KMeansModel cluster(DenseLocalOnHeapMatrix points, int k,
        List<Double> weights) throws MathIllegalArgumentException, ConvergenceException {

        GridArgumentCheck.notNull(points, "points");

        int dim = points.columnSize();
        Vector[] centers = new Vector[k];

        centers[0] = pickWeighted(points, weights);

        Vector costs = points.foldRows(row -> distance(row,
            centers[0]));

        for (int i = 0; i < k; i++) {
            double weightedSum = weightedSum(costs, weights);

            double r = rand.nextDouble() * weightedSum;
            double s = 0.0;
            int j = 0;

            while (j < points.rowSize() && s < r) {
                s += weights.get(j) * costs.get(j);
                j++;
            }

            if (j == 0)
                // TODO: Process this case more carefully
                centers[i] = localCopyOf(points.viewRow(0));
            else
                centers[i] = localCopyOf(points.viewRow(j - 1));

            for (int p = 0; p < points.rowSize(); p++)
                costs.setX(p, Math.min(getDistanceMeasure().compute(localCopyOf(points.viewRow(p)), centers[i]),
                        costs.get(p)));
        }

        int[] oldClosest = new int[points.rowSize()];
        Arrays.fill(oldClosest, -1);
        int iter = 0;
        boolean moved = true;

        while (moved && iter < maxIterations) {
            moved = false;

            double[] counts = new double[k];
            Arrays.fill(counts, 0.0);
            Vector[] sums = new Vector[k];

            Arrays.fill(sums, VectorUtils.zeroes(dim));

            int i = 0;

            while (i < points.rowSize()) {
                Vector p = localCopyOf(points.viewRow(i));

                int ind = findClosest(centers, p).get1();
                sums[ind] = sums[ind].plus(p.times(weights.get(i)));

                counts[ind] += weights.get(i);
                if (ind != oldClosest[i]) {
                    moved = true;
                    oldClosest[i] = ind;
                }
                i++;
            }
            // Update centers
            int j = 0;
            while (j < k) {
                if (counts[j] == 0.0) {
                    // Assign center to a random point
                    centers[j] = points.viewRow(rand.nextInt(points.rowSize()));
                } else {
                    sums[j] = sums[j].times(1.0 / counts[j]);
                    centers[j] = sums[j];
                }
                j++;
            }
            iter++;
        }

        return new KMeansModel(centers, getDistanceMeasure());
    }

    /** Pick a random vector with a probability proportional to the corresponding weight. */
    private Vector pickWeighted(Matrix points, List<Double> weights) {
        double r = rand.nextDouble() * weights.stream().mapToDouble(Double::valueOf).sum();

        int i = 0;
        double curWeight = 0.0;

        while (i < points.rowSize() && curWeight < r) {
            curWeight += weights.get(i);
            i += 1;
        }

        return localCopyOf(points.viewRow(i - 1));
    }

    /** Get a weighted sum of a vector v. */
    private double weightedSum(Vector v, List<Double> weights) {
        double res = 0.0;

        for (int i = 0; i < v.size(); i++)
            res += v.getX(i) * weights.get(i);

        return res;
    }
}
