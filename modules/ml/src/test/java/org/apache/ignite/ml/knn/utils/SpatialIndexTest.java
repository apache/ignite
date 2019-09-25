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

package org.apache.ignite.ml.knn.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for {@link SpatialIndex} implementation tests.
 */
public abstract class SpatialIndexTest {
    /** Distance measure to be used in tests. */
    private static final DistanceMeasure distanceMeasure = new EuclideanDistance();

    /** Spatial index factory. */
    private final BiFunction<List<LabeledVector<Integer>>, DistanceMeasure, SpatialIndex<Integer>> idxFactory;

    /**
     * Constructs a new instance of spatial index test.
     *
     * @param idxFactory Factory that produces spatial indices.
     */
    SpatialIndexTest(BiFunction<List<LabeledVector<Integer>>, DistanceMeasure, SpatialIndex<Integer>> idxFactory) {
        this.idxFactory = idxFactory;
    }

    /** */
    @Test
    public void testFindKClosestInOneDimension() {
        List<LabeledVector<Integer>> dataset = new ArrayList<>();
        for (int i = -100; i <= 100; i++) {
            LabeledVector<Integer> vectorPnt = VectorUtils.of(i).labeled(0);
            dataset.add(vectorPnt);
        }

        SpatialIndex<Integer> idx = idxFactory.apply(dataset, distanceMeasure);

        for (int k = 1; k <= 201; k += 2) {
            double expRadius = 1.0 * (k - 1) / 2;
            Vector pnt = VectorUtils.of(0);
            List<LabeledVector<Integer>> neighbours = idx.findKClosest(k, pnt);

            assertEquals(k, neighbours.size());
            for (LabeledVector<Integer> neighbour : neighbours) {
                double distance = distanceMeasure.compute(pnt, neighbour.features());
                assertTrue(distance <= expRadius);
            }
        }
    }

    /** */
    @Test
    public void testFindKClosestInThreeDimensions() {
        List<LabeledVector<Integer>> dataset = new ArrayList<>();
        for (int d1 = -10; d1 <= 10; d1++) {
            for (int d2 = -10; d2 <= 10; d2++) {
                for (int d3 = -10; d3 <= 10; d3++) {
                    LabeledVector<Integer> vectorPnt = VectorUtils.of(d1, d2, d3).labeled(0);
                    dataset.add(vectorPnt);
                }
            }
        }

        SpatialIndex<Integer> idx = idxFactory.apply(dataset, distanceMeasure);

        for (int k = 1; k <= 81; k += 8) {
            double expRadius = Math.sqrt(3) * (k - 1) / 8;
            Vector pnt = VectorUtils.of(0, 0, 0);
            List<LabeledVector<Integer>> neighbours = idx.findKClosest(k, pnt);

            assertEquals(k, neighbours.size());
            for (LabeledVector<Integer> neighbour : neighbours) {
                double distance = distanceMeasure.compute(pnt, neighbour.features());
                assertTrue(distance <= expRadius);
            }
        }
    }
}
