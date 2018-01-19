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
import java.util.Comparator;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests that checks local Fuzzy C-Means clusterer. */
public class FuzzyCMeansLocalClustererTest {
    /** Test FCM on points that forms three clusters on the line. */
    @Test
    public void equalWeightsOneDimension() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS,
                0.01, 10, null);

        double[][] points = new double[][]{{-10}, {-9}, {-8}, {-7},
                                           {7},   {8},  {9},  {10},
                                           {-1},  {0},  {1}};

        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);

        FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, 3);

        Vector[] centers = mdl.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> vector.getX(0)));
        assertEquals(-8.5, centers[0].getX(0), 2);
        assertEquals(0, centers[1].getX(0), 2);
        assertEquals(8.5, centers[2].getX(0), 2);
    }

    /** Test FCM on points that forms four clusters on the plane. */
    @Test
    public void equalWeightsTwoDimensions() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS,
                0.01, 20, null);

        double[][] points = new double[][]{{-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
                                           {10, 10},   {9, 11},   {10, 9},   {11, 9},
                                           {-10, 10},  {-9, 11},  {-10, 9},  {-11, 9},
                                           {10, -10},  {9, -11},  {10, -9},  {11, -9}};

        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);

        FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, 4);
        Vector[] centers = mdl.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> Math.atan2(vector.get(1), vector.get(0))));

        DistanceMeasure measure = mdl.distanceMeasure();

        assertEquals(0, measure.compute(centers[0], new DenseLocalOnHeapVector(new double[]{-10, -10})), 1);
        assertEquals(0, measure.compute(centers[1], new DenseLocalOnHeapVector(new double[]{10, -10})), 1);
        assertEquals(0, measure.compute(centers[2], new DenseLocalOnHeapVector(new double[]{10, 10})), 1);
        assertEquals(0, measure.compute(centers[3], new DenseLocalOnHeapVector(new double[]{-10, 10})), 1);
    }

    /** Test FCM on points which have the equal coordinates. */
    @Test
    public void checkCentersOfTheSamePointsTwoDimensions() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_MEMBERSHIPS, 0.01, 10, null);

        double[][] points = new double[][] {{3.3, 10}, {3.3, 10}, {3.3, 10}, {3.3, 10}, {3.3, 10}};

        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 2;
        FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, k);
        Vector exp = new DenseLocalOnHeapVector(new double[] {3.3, 10});
        for (int i = 0; i < k; i++) {
            Vector center = mdl.centers()[i];

            for (int j = 0; j < 2; j++)
                assertEquals(exp.getX(j), center.getX(j), 1);
        }
    }

    /** Test FCM on points located on the circle. */
    @Test
    public void checkCentersLocationOnSphere() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS, 0.01, 100, null);

        int numOfPoints = 650;
        double radius = 100.0;
        double[][] points = new double [numOfPoints][2];

        for (int i = 0; i < numOfPoints; i++) {
            points[i][0] = Math.cos(Math.PI * 2 * i / numOfPoints) * radius;
            points[i][1] = Math.sin(Math.PI * 2 * i / numOfPoints) * radius;
        }

        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 10;
        FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, k);

        Vector sum = mdl.centers()[0];
        for (int i = 1; i < k; i++)
            sum = sum.plus(mdl.centers()[i]);

        assertEquals(0, sum.kNorm(1), 1);
    }

    /** Test FCM on points that forms the line located on the plane. */
    @Test
    public void test2DLineClustering() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS, 0.01, 50, null);

        double[][] points = new double[][]{{1, 2}, {3, 6}, {5, 10}};

        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 2;
        FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, k);
        Vector[] centers = mdl.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> vector.getX(0)));

        Vector[] exp = {new DenseLocalOnHeapVector(new double[]{1.5, 3}),
                        new DenseLocalOnHeapVector(new double[]{4.5, 9})};

        for (int i = 0; i < k; i++) {
            Vector center = centers[i];

            for (int j = 0; j < 2; j++)
                assertEquals(exp[i].getX(j), center.getX(j), 0.5);
        }
    }

    /** Test FCM on points that have different weights. */
    @Test
    public void differentWeightsOneDimension() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS,
                0.01, 10, null);

        double[][] points = new double[][]{{1}, {2}, {3}, {4}, {5}, {6}};

        DenseLocalOnHeapMatrix pntMatrix = new DenseLocalOnHeapMatrix(points);
        ArrayList<Double> weights = new ArrayList<>();
        Collections.addAll(weights, 3.0, 2.0, 1.0, 1.0, 1.0, 1.0);

        Vector[] centers1 = clusterer.cluster(pntMatrix, 2).centers();
        Vector[] centers2 = clusterer.cluster(pntMatrix, 2, weights).centers();
        Arrays.sort(centers1, Comparator.comparing(vector -> vector.getX(0)));
        Arrays.sort(centers2, Comparator.comparing(vector -> vector.getX(0)));

        assertTrue(centers1[0].get(0) - centers2[0].get(0) > 0.5);
    }

    /** Test FCM on illegal number of clusters. */
    @Test(expected = MathIllegalArgumentException.class)
    public void testIllegalNumberOfClusters() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS, 0.01, 10, null);
        double[][] points = new double[][]{{1}, {2}, {3}, {4}};

        clusterer.cluster(new DenseLocalOnHeapMatrix(points), 1);
    }

    /** Test FCM on different numbers of points and weights. */
    @Test(expected = MathIllegalArgumentException.class)
    public void testDifferentAmountsOfPointsAndWeights(){
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS, 0.01, 10, null);
        double[][] points = new double[][]{{1}, {2}, {3}, {4}};

        ArrayList<Double> weights = new ArrayList<>();
        Collections.addAll(weights, 1.0, 34.0, 2.5, 5.0, 0.5);

        clusterer.cluster(new DenseLocalOnHeapMatrix(points), 2, weights);
    }
}
