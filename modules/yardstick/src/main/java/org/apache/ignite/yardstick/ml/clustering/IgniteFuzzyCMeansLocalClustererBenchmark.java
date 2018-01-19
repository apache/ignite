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
import org.apache.ignite.ml.clustering.BaseFuzzyCMeansClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansLocalClusterer;
import org.apache.ignite.ml.clustering.FuzzyCMeansModel;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.ml.DataChanger;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
@SuppressWarnings("unused")
public class IgniteFuzzyCMeansLocalClustererBenchmark extends IgniteAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        // IMPL NOTE originally taken from FuzzyLocalCMeansExample.
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

        // Create new local clusterer with parameters described above.
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(distanceMeasure,
            exponentialWeight, stopCond, maxDelta, maxIterations, null);

        // Create sample data.
        double[][] points = shuffle((int)(DataChanger.next()));

        // Create the matrix that contains sample points.
        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);

        // Call clusterization method with some number of centers.
        // It returns model that can predict results for new points.
        int numCenters = 4;
        FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, numCenters);

        // Get centers of clusters that is computed by Fuzzy C-Means algorithm.
        mdl.centers();

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
