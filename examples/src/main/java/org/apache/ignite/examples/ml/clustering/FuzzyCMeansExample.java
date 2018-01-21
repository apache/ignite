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

package org.apache.ignite.examples.ml.clustering;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.ml.clustering.BaseFuzzyCMeansClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansDistributedClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansModel;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.thread.IgniteThread;

/**
 * <p>
 * This example shows how to use {@link FuzzyCMeansDistributedClusterer}.</p>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p>
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public final class FuzzyCMeansExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println(">>> Fuzzy C-Means usage example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Start new Ignite thread.
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                FuzzyCMeansExample.class.getSimpleName(),
                () -> {
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
                    System.out.println(">>> Create new Distributed Fuzzy C-Means clusterer.");
                    FuzzyCMeansDistributedClusterer clusterer = new FuzzyCMeansDistributedClusterer(
                        distanceMeasure, exponentialWeight, stopCond, maxDelta, maxIterations,
                        seed, initializationSteps, kMeansMaxIterations);

                    // Create sample data.
                    double[][] points = new double[][] {
                        {-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
                        {10, 10}, {9, 11}, {10, 9}, {11, 9},
                        {-10, 10}, {-9, 11}, {-10, 9}, {-11, 9},
                        {10, -10}, {9, -11}, {10, -9}, {11, -9}};

                    // Initialize matrix of data points. Each row contains one point.
                    int rows = points.length;
                    int cols = points[0].length;

                    System.out.println(">>> Create the matrix that contains sample points.");
                    SparseDistributedMatrix pntMatrix = new SparseDistributedMatrix(rows, cols,
                        StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

                    // Store points into matrix.
                    pntMatrix.assign(points);

                    // Call clusterization method with some number of centers.
                    // It returns model that can predict results for new points.
                    System.out.println(">>> Perform clusterization.");
                    int numCenters = 4;
                    FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, numCenters);

                    // You can also get centers of clusters that is computed by Fuzzy C-Means algorithm.
                    Vector[] centers = mdl.centers();

                    String res = ">>> Results:\n"
                        + ">>> 1st center: " + centers[0].get(0) + " " + centers[0].get(1) + "\n"
                        + ">>> 2nd center: " + centers[1].get(0) + " " + centers[1].get(1) + "\n"
                        + ">>> 3rd center: " + centers[2].get(0) + " " + centers[2].get(1) + "\n"
                        + ">>> 4th center: " + centers[3].get(0) + " " + centers[3].get(1) + "\n";

                    System.out.println(res);

                    pntMatrix.destroy();
                });

            igniteThread.start();
            igniteThread.join();
        }
    }
}
