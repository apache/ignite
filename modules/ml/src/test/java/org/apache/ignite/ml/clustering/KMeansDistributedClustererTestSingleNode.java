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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.EuclideanDistance;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.ml.clustering.KMeansUtil.checkIsInEpsilonNeighbourhood;

/**
 * This test checks logic of clustering (checks for clusters structures).
 */
public class KMeansDistributedClustererTestSingleNode extends GridCommonAbstractTest {
    /**
     * Number of nodes in grid. We should use 1 in this test because otherwise algorithm will be unstable
     * (We cannot guarantee the order in which results are returned from each node).
     */
    private static final int NODE_COUNT = 1;

    /** Grid instance. */
    private Ignite ignite;

    /**
     * Default constructor.
     */
    public KMeansDistributedClustererTestSingleNode() {
        super(false);
    }

    /**
     * {@inheritDoc}
     */
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

    /** */
    public void testPerformClusterAnalysisDegenerate() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        KMeansDistributedClusterer clusterer = new KMeansDistributedClusterer(new EuclideanDistance(), 1, 1, 1L);

        double[] v1 = new double[] {1959, 325100};
        double[] v2 = new double[] {1960, 373200};

        SparseDistributedMatrix points = new SparseDistributedMatrix(2, 2, StorageConstants.ROW_STORAGE_MODE,
            StorageConstants.RANDOM_ACCESS_MODE);

        points.setRow(0, v1);
        points.setRow(1, v2);

        KMeansModel mdl = clusterer.cluster(points, 1);

        Assert.assertEquals(1, mdl.centers().length);
        Assert.assertEquals(2, mdl.centers()[0].size());
    }

    /** */
    public void testClusterizationOnDatasetWithObviousStructure() throws IOException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        int ptsCnt = 10000;
        int squareSideLen = 10000;

        Random rnd = new Random(123456L);

        // Let centers be in the vertices of square.
        Map<Integer, Vector> centers = new HashMap<>();
        centers.put(100, new DenseLocalOnHeapVector(new double[] {0.0, 0.0}));
        centers.put(900, new DenseLocalOnHeapVector(new double[] {squareSideLen, 0.0}));
        centers.put(3000, new DenseLocalOnHeapVector(new double[] {0.0, squareSideLen}));
        centers.put(6000, new DenseLocalOnHeapVector(new double[] {squareSideLen, squareSideLen}));

        int centersCnt = centers.size();

        SparseDistributedMatrix points = new SparseDistributedMatrix(ptsCnt, 2, StorageConstants.ROW_STORAGE_MODE,
            StorageConstants.RANDOM_ACCESS_MODE);

        List<Integer> permutation = IntStream.range(0, ptsCnt).boxed().collect(Collectors.toList());
        Collections.shuffle(permutation, rnd);

        Vector[] mc = new Vector[centersCnt];
        Arrays.fill(mc, VectorUtils.zeroes(2));

        int centIdx = 0;
        int totalCnt = 0;

        List<Vector> massCenters = new ArrayList<>();

        for (Integer count : centers.keySet()) {
            for (int i = 0; i < count; i++) {
                Vector pnt = new DenseLocalOnHeapVector(2).assign(centers.get(count));
                // Perturbate point on random value.
                pnt.map(val -> val + rnd.nextDouble() * squareSideLen / 100);
                mc[centIdx] = mc[centIdx].plus(pnt);
                points.assignRow(permutation.get(totalCnt), pnt);
                totalCnt++;
            }
            massCenters.add(mc[centIdx].times(1 / (double)count));
            centIdx++;
        }

        EuclideanDistance dist = new EuclideanDistance();
        OrderedNodesComparator comp = new OrderedNodesComparator(centers.values().toArray(new Vector[] {}), dist);

        massCenters.sort(comp);
        KMeansDistributedClusterer clusterer = new KMeansDistributedClusterer(dist, 3, 100, 1L);

        KMeansModel mdl = clusterer.cluster(points, 4);
        Vector[] resCenters = mdl.centers();
        Arrays.sort(resCenters, comp);

        checkIsInEpsilonNeighbourhood(resCenters, massCenters.toArray(new Vector[] {}), 30.0);

        points.destroy();
    }

    /** */
    private static class OrderedNodesComparator implements Comparator<Vector> {
        /** */
        private final DistanceMeasure measure;

        /** */
        List<Vector> orderedNodes;

        /** */
        OrderedNodesComparator(Vector[] orderedNodes, DistanceMeasure measure) {
            this.orderedNodes = Arrays.asList(orderedNodes);
            this.measure = measure;
        }

        /** */
        private int findClosestNodeIndex(Vector v) {
            return Functions.argmin(orderedNodes, v1 -> measure.compute(v1, v)).get1();
        }

        /** */
        @Override public int compare(Vector v1, Vector v2) {
            int ind1 = findClosestNodeIndex(v1);
            int ind2 = findClosestNodeIndex(v2);

            int signum = (int)Math.signum(ind1 - ind2);

            if (signum != 0)
                return signum;

            return (int)Math.signum(orderedNodes.get(ind1).minus(v1).kNorm(2) -
                orderedNodes.get(ind2).minus(v2).kNorm(2));
        }
    }
}
