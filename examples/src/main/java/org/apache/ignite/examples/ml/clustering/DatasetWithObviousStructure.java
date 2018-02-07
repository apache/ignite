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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * See KMeansDistributedClustererTestSingleNode#testClusterizationOnDatasetWithObviousStructure.
 */
class DatasetWithObviousStructure {
    /** */
    private final Random rnd = new Random(123456L);

    /** Let centers be in the vertices of square. */
    private final Map<Integer, Vector> centers = new HashMap<>();

    /** Square side length. */
    private final int squareSideLen;

    /** */
    DatasetWithObviousStructure(int squareSideLen) {
        this.squareSideLen = squareSideLen;
        centers.put(100, new DenseLocalOnHeapVector(new double[] {0.0, 0.0}));
        centers.put(900, new DenseLocalOnHeapVector(new double[] {squareSideLen, 0.0}));
        centers.put(3000, new DenseLocalOnHeapVector(new double[] {0.0, squareSideLen}));
        centers.put(6000, new DenseLocalOnHeapVector(new double[] {squareSideLen, squareSideLen}));
    }

    /** */
    List<Vector> generate(Matrix points) {
        int ptsCnt = points.rowSize();

        // Mass centers of dataset.
        List<Vector> massCenters = new ArrayList<>();

        int centersCnt = centers.size();

        List<Integer> permutation = IntStream.range(0, ptsCnt).boxed().collect(Collectors.toList());
        Collections.shuffle(permutation, rnd);

        Vector[] mc = new Vector[centersCnt];
        Arrays.fill(mc, VectorUtils.zeroes(2));

        int totalCnt = 0;

        int centIdx = 0;
        massCenters.clear();

        for (Integer count : centers.keySet()) {
            for (int i = 0; i < count; i++) {
                Vector pnt = getPoint(count);

                mc[centIdx] = mc[centIdx].plus(pnt);

                points.assignRow(permutation.get(totalCnt), pnt);

                totalCnt++;
            }
            massCenters.add(mc[centIdx].times(1 / (double)count));
            centIdx++;
        }

        return massCenters;
    }

    /** */
    Map<Integer, Vector> centers() {
        return centers;
    }

    /** */
    private Vector getPoint(Integer cnt) {
        Vector pnt = new DenseLocalOnHeapVector(2).assign(centers.get(cnt));
        // Perturbate point on random value.
        pnt.map(val -> val + rnd.nextDouble() * squareSideLen / 100);
        return pnt;
    }
}
