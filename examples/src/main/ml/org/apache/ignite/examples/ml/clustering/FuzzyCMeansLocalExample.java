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

import org.apache.ignite.ml.clustering.BaseFuzzyCMeansClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansLocalClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansModel;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * This example shows how to use {@link FuzzyCMeansLocalClusterer}.
 */
public final class FuzzyCMeansLocalExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println(">>> Local Fuzzy C-Means usage example started.");

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

        // Create new distributed clusterer with parameters described above.
        System.out.println(">>> Create new Local Fuzzy C-Means clusterer.");
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(distanceMeasure,
            exponentialWeight, stopCond,
            maxDelta, maxIterations, seed);

        // Create sample data.
        double[][] points = new double[][] {
            {-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
            {10, 10}, {9, 11}, {10, 9}, {11, 9},
            {-10, 10}, {-9, 11}, {-10, 9}, {-11, 9},
            {10, -10}, {9, -11}, {10, -9}, {11, -9}};

        // Initialize matrix of data points. Each row contains one point.
        System.out.println(">>> Create the matrix that contains sample points.");
        // Store points into matrix.
        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);

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
    }
}
