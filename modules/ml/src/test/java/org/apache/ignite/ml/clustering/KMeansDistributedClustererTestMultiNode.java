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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * This test is made to make sure that K-Means distributed clustering does not crush on distributed environment.
 * In {@link KMeansDistributedClustererTestMultiNode} we check logic of clustering (checks for clusters structures).
 * In this class we just check that clusterer does not crush. There are two separate tests because we cannot
 * guarantee order in which nodes return results of intermediate computations and therefore algorithm can return
 * different results.
 */
public class KMeansDistributedClustererTestMultiNode extends GridCommonAbstractTest {
    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 3;

    /** Grid instance. */
    private Ignite ignite;

    /**
     * Default constructor.
     */
    public KMeansDistributedClustererTestMultiNode() {
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
    @Test
    public void testPerformClusterAnalysisDegenerate() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        KMeansDistributedClusterer clusterer = new KMeansDistributedClusterer(new EuclideanDistance(), 1, 1, 1L);

        double[] v1 = new double[] {1959, 325100};
        double[] v2 = new double[] {1960, 373200};

        SparseDistributedMatrix points = new SparseDistributedMatrix(2, 2, StorageConstants.ROW_STORAGE_MODE,
            StorageConstants.RANDOM_ACCESS_MODE);

        points.setRow(0, v1);
        points.setRow(1, v2);

        clusterer.cluster(points, 1);
    }

    /** */
    @Test
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

        SparseDistributedMatrix points = new SparseDistributedMatrix(ptsCnt, 2, StorageConstants.ROW_STORAGE_MODE,
        StorageConstants.RANDOM_ACCESS_MODE);

        List<Integer> permutation = IntStream.range(0, ptsCnt).boxed().collect(Collectors.toList());
        Collections.shuffle(permutation, rnd);

        int totalCnt = 0;

        for (Integer count : centers.keySet()) {
            for (int i = 0; i < count; i++) {
                DenseLocalOnHeapVector pnt = (DenseLocalOnHeapVector)new DenseLocalOnHeapVector(2).assign(centers.get(count));
                // Perturbate point on random value.
                pnt.map(val -> val + rnd.nextDouble() * squareSideLen / 100);
                points.assignRow(permutation.get(totalCnt), pnt);
                totalCnt++;
            }
        }

        EuclideanDistance dist = new EuclideanDistance();

        KMeansDistributedClusterer clusterer = new KMeansDistributedClusterer(dist, 3, 100, 1L);

        clusterer.cluster(points, 4);
    }
}