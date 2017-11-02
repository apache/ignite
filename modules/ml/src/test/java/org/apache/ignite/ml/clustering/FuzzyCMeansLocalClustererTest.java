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

import org.apache.ignite.ml.math.*;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import static org.junit.Assert.*;

public class FuzzyCMeansLocalClustererTest {

    /** test FCM for points that forms three clusters on the line */
    @Test
    public void equalWeightsOneDimension() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS,
                0.01, 10, null);

        double[][] points = new double[][]{{-10}, {-9}, {-8}, {-7},
                                           {7},   {8},  {9},  {10},
                                           {-1},  {0},  {1}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, 3);

        Vector[] centers = model.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> vector.getX(0)));
        assertEquals(-8.5, centers[0].getX(0), 2);
        assertEquals(0, centers[1].getX(0), 2);
        assertEquals(8.5, centers[2].getX(0), 2);
    }

    /** test FCM for points that forms four clusters on the plane */
    @Test
    public void equalWeightsTwoDimensions() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS,
                0.01, 20, null);

        double[][] points = new double[][]{{-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
                                           {10, 10},   {9, 11},   {10, 9},   {11, 9},
                                           {-10, 10},  {-9, 11},  {-10, 9},  {-11, 9},
                                           {10, -10},  {9, -11},  {10, -9},  {11, -9}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, 4);
        Vector[] centers = model.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> Math.atan2(vector.get(1), vector.get(0))));

        DistanceMeasure measure = model.distanceMeasure();

        assertEquals(0, measure.compute(centers[0], new DenseLocalOnHeapVector(new double[]{-10, -10})), 1);
        assertEquals(0, measure.compute(centers[1], new DenseLocalOnHeapVector(new double[]{10, -10})), 1);
        assertEquals(0, measure.compute(centers[2], new DenseLocalOnHeapVector(new double[]{10, 10})), 1);
        assertEquals(0, measure.compute(centers[3], new DenseLocalOnHeapVector(new double[]{-10, 10})), 1);
    }

    /** test FCM for points with same coordinates*/
    @Test
    public void correctCentersOfTheSamePointsTwoDimensions() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);

        double[][] points = new double[][] {{3.3, 10}, {3.3, 10}, {3.3, 10}, {3.3, 10}, {3.3, 10}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 2;
        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, k);
        Vector expected = new DenseLocalOnHeapVector(new double[] {3.3, 10});
        for (int i = 0; i < k; i++) {
            Vector center = model.centers()[i];

            for (int j = 0; j < 2; j++) {
                assertEquals(expected.getX(j), center.getX(j), 1);
            }
        }
    }

    /** test FCM for points on sphere*/
    @Test
    public void correctDefinitionOfCentersOnSphrere() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 100, null);

        int numberOfCenters = 65;
        double radius = 10.0;
        double[][] points = new double [numberOfCenters][2];
        for (int i = 0; i < numberOfCenters; i++) {
            points[i][0] = Math.cos(Math.PI*2*i/numberOfCenters) * radius;
            points[i][1] = Math.sin(Math.PI*2*i/numberOfCenters) * radius;
        }

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 10;
        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, k);

        Vector sum = model.centers()[0];
        for (int i = 1; i < k; i++) {
            sum = sum.plus(model.centers()[i]);
        }
        assertEquals(0, sum.kNorm(1), 0.5);
    }

    /** test FCM for points on line*/
    @Test
    public void test2DLineClustering() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 50, null);

        double[][] points = new double[][]{{1, 2}, {3, 6}, {5, 10}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 2;
        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, k);
        Vector[] centers = model.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> vector.getX(0)));

        Vector[] expected = {new DenseLocalOnHeapVector(new double[]{1.5, 3}),
                             new DenseLocalOnHeapVector(new double[]{4.5, 9})};

        for (int i = 0; i < k; i++) {
            Vector center = centers[i];

            for (int j = 0; j < 2; j++) {
                assertEquals(expected[i].getX(j), center.getX(j), 0.5);
            }
        }
    }

    /** test FCM for points that have different weights */
    @Test
    public void differentWeightsOneDimension() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS,
                0.01, 10, null);

        double[][] points = new double[][]{{1}, {2}, {3}, {4}, {5}, {6}};

        DenseLocalOnHeapMatrix pointMatrix = new DenseLocalOnHeapMatrix(points);
        ArrayList<Double> weights = new ArrayList<>();
        Collections.addAll(weights, new Double[]{3.0, 2.0, 1.0, 1.0, 1.0, 1.0});

        Vector[] centers1 = clusterer.cluster(pointMatrix, 2).centers();
        Vector[] centers2 = clusterer.cluster(pointMatrix, 2, weights).centers();
        Arrays.sort(centers1, Comparator.comparing(vector -> vector.getX(0)));
        Arrays.sort(centers2, Comparator.comparing(vector -> vector.getX(0)));

        assertTrue(centers1[0].get(0) - centers2[0].get(0) > 0.5);
    }

    /** test FCM for illegal number of clusters*/
    @Test(expected = MathIllegalArgumentException.class)
    public void testClusteringWithOneCenter() {
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);
        double[][] points = new double[][]{{1}, {2}, {3}, {4}};

        FuzzyCMeansModel cluster = clusterer.cluster(new DenseLocalOnHeapMatrix(points), 1);
    }

    /** test FCM for different amounts of points and weights*/
    @Test(expected = MathIllegalArgumentException.class)
    public void testClusteringWithDifferentAmountsOfPointsAndWeights(){
        FuzzyCMeansLocalClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);
        double[][] points = new double[][]{{1}, {2}, {3}, {4}};
        ArrayList<Double> weights = new ArrayList<>();
        Collections.addAll(weights, new Double[]{1.0, 34.0, 2.5, 5.0, 0.5});
        FuzzyCMeansModel cluster = clusterer.cluster(new DenseLocalOnHeapMatrix(points), 2, weights);
    }
}
