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

package org.apache.ignite.yardstick.ml.clustering;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.clustering.BaseFuzzyCMeansClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansDistributedClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansModel;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.ml.DataChanger;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
@SuppressWarnings("unused")
public class IgniteFuzzyCMeansDistributedClustererBenchmark extends IgniteAbstractBenchmark {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
        // because we create ignite cache internally.
        IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
            this.getClass().getSimpleName(), new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                // IMPL NOTE originally taken from FuzzyCMeansExample.
                // Distance measure that computes distance between two points.
                DistanceMeasure distanceMeasure = new EuclideanDistance();

                // "Fuzziness" - specific constant that is used in membership calculation (1.0+-eps ~ K-Means).
                double exponentialWeight = 2.0;

                // Condition that indicated when algorithm must stop.
                // In this example algorithm stops if memberships have changed insignificantly.
                BaseFuzzyCMeansClusterer.StopCondition stopCond =
                    BaseFuzzyCMeansClusterer.StopCondition.STABLE_MEMBERSHIPS;

                // Maximum difference between new and old membership values with which algorithm will continue to work.
                double maxDelta = 0.01;

                // The maximum number of FCM iterations.
                int maxIterations = 50;

                // Value that is used to initialize random numbers generator. You can choose it randomly.
                Long seed = null;

                // Number of steps of primary centers selection (more steps more candidates).
                int initializationSteps = 2;

                // Number of K-Means iteration that is used to choose required number of primary centers from candidates.
                int kMeansMaxIterations = 50;

                // Create new distributed clusterer with parameters described above.
                FuzzyCMeansDistributedClusterer clusterer = new FuzzyCMeansDistributedClusterer(
                    distanceMeasure, exponentialWeight, stopCond, maxDelta, maxIterations,
                    seed, initializationSteps, kMeansMaxIterations);

                // Create sample data.
                double[][] points = shuffle((int)(DataChanger.next()));

                // Initialize matrix of data points. Each row contains one point.
                int rows = points.length;
                int cols = points[0].length;

                // Create the matrix that contains sample points.
                SparseDistributedMatrix pntMatrix = new SparseDistributedMatrix(rows, cols,
                    StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

                // Store points into matrix.
                pntMatrix.assign(points);

                // Call clusterization method with some number of centers.
                // It returns model that can predict results for new points.
                int numCenters = 4;
                FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, numCenters);

                // Get centers of clusters that is computed by Fuzzy C-Means algorithm.
                mdl.centers();

                pntMatrix.destroy();
            }
        });

        igniteThread.start();

        igniteThread.join();

        return true;
    }

    /** */
    private double[][] shuffle(int off) {
        final double[][] points = new double[][] {
            {-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
            {10, 10}, {9, 11}, {10, 9}, {11, 9},
            {-10, 10}, {-9, 11}, {-10, 9}, {-11, 9},
            {10, -10}, {9, -11}, {10, -9}, {11, -9}};

        final int size = points.length;

        final double[][] res = new double[size][];

        for (int i = 0; i < size; i++)
            res[i] = points[(i + off) % size];

        return res;
    }
}
