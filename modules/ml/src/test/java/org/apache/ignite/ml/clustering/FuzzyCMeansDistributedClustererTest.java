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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** Tests that checks distributed Fuzzy C-Means clusterer. */
public class FuzzyCMeansDistributedClustererTest extends GridCommonAbstractTest {
    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 3;

    /** Grid instance. */
    private Ignite ignite;

    /** Default constructor. */
    public FuzzyCMeansDistributedClustererTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** Test that algorithm gives correct results on a small sample - 4 centers on the plane. */
    public void testTwoDimensionsLittleData() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        FuzzyCMeansDistributedClusterer clusterer = new FuzzyCMeansDistributedClusterer(new EuclideanDistance(),
                2, BaseFuzzyCMeansClusterer.StopCondition.STABLE_MEMBERSHIPS,
                0.01, 500, null, 2, 50);

        double[][] points = new double[][]{{-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
                {10, 10},   {9, 11},   {10, 9},   {11, 9},
                {-10, 10},  {-9, 11},  {-10, 9},  {-11, 9},
                {10, -10},  {9, -11},  {10, -9},  {11, -9}};

        SparseDistributedMatrix pntMatrix = new SparseDistributedMatrix(16, 2,
                StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
        for (int i = 0; i < 16; i++)
            pntMatrix.setRow(i, points[i]);

        FuzzyCMeansModel mdl = clusterer.cluster(pntMatrix, 4);

        Vector[] centers = mdl.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> Math.atan2(vector.get(1), vector.get(0))));

        DistanceMeasure measure = mdl.distanceMeasure();

        assertEquals(0, measure.compute(centers[0], new DenseLocalOnHeapVector(new double[]{-10, -10})), 1);
        assertEquals(0, measure.compute(centers[1], new DenseLocalOnHeapVector(new double[]{10, -10})), 1);
        assertEquals(0, measure.compute(centers[2], new DenseLocalOnHeapVector(new double[]{10, 10})), 1);
        assertEquals(0, measure.compute(centers[3], new DenseLocalOnHeapVector(new double[]{-10, 10})), 1);
    }

    /** Perform N tests each of which contains M random points placed around K centers on the plane. */
    public void testTwoDimensionsRandomlyPlacedPointsAndCenters() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        final int numOfTests = 5;

        final double exponentialWeight = 2.0;
        final double maxCentersDelta = 0.01;
        final int maxIterations = 500;
        final Long seed = 1L;

        DistanceMeasure measure = new EuclideanDistance();
        FuzzyCMeansDistributedClusterer distributedClusterer = new FuzzyCMeansDistributedClusterer(measure,
                exponentialWeight, BaseFuzzyCMeansClusterer.StopCondition.STABLE_CENTERS,
                maxCentersDelta, maxIterations, seed, 2, 50);

        for (int i = 0; i < numOfTests; i++)
            performRandomTest(distributedClusterer, i);
    }

    /**
     * Test given clusterer on points placed randomly around vertexes of a regular polygon.
     *
     * @param distributedClusterer Tested clusterer.
     * @param seed Seed for the random numbers generator.
     */
    public void performRandomTest(FuzzyCMeansDistributedClusterer distributedClusterer, long seed) {
        final int minNumCenters = 2;
        final int maxNumCenters = 5;
        final double maxRadius = 1000;
        final int maxPoints = 1000;
        final int minPoints = 300;

        Random random = new Random(seed);

        int numCenters = random.nextInt(maxNumCenters - minNumCenters) + minNumCenters;

        double[][] centers = new double[numCenters][2];

        for (int i = 0; i < numCenters; i++) {
            double radius = maxRadius;
            double angle = Math.PI * 2.0 * i / numCenters;

            centers[i][0] = Math.cos(angle) * radius;
            centers[i][1] = Math.sin(angle) * radius;
        }

        int numPoints = minPoints + random.nextInt(maxPoints - minPoints);

        double[][] points = new double[numPoints][2];

        for (int i = 0; i < numPoints; i++) {
            int center = random.nextInt(numCenters);
            double randomDouble = random.nextDouble();
            double radius = randomDouble * randomDouble * maxRadius / 10;
            double angle = random.nextDouble() * Math.PI * 2.0;

            points[i][0] = centers[center][0] + Math.cos(angle) * radius;
            points[i][1] = centers[center][1] + Math.sin(angle) * radius;
        }

        SparseDistributedMatrix pntMatrix = new SparseDistributedMatrix(numPoints, 2,
                StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < numPoints; i++)
            pntMatrix.setRow(i, points[i]);

        FuzzyCMeansModel mdl = distributedClusterer.cluster(pntMatrix, numCenters);
        Vector[] computedCenters = mdl.centers();
        DistanceMeasure measure = mdl.distanceMeasure();

        int cntr = numCenters;

        for (int i = 0; i < numCenters; i++) {
            for (int j = 0; j < numCenters; j++) {
                if (measure.compute(computedCenters[i], new DenseLocalOnHeapVector(centers[j])) < 100) {
                    cntr--;
                    break;
                }
            }
        }

        assertEquals(0, cntr);
    }
}
