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

package org.apache.ignite.ml.knn.utils.indices;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.ml.knn.utils.PointWithDistance;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.transformToListOrdered;
import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.tryToAddIntoHeap;

/**
 * Array based implementation of {@link SpatialIndex}. Asymptotic runtime complexity of finding {@code k} closest
 * elements is {@code O(n*log(k))}, but it doesn't degrade on high dimensional data.
 *
 * @param <L> Label type.
 */
public class ArraySpatialIndex<L> implements SpatialIndex<L> {
    /** Data. */
    private final List<LabeledVector<L>> data;

    /** Distance measure. */
    private final DistanceMeasure distanceMeasure;

    /**
     * Construct a new array spatial index.
     *
     * @param data Data.
     * @param distanceMeasure Distance measure.
     */
    public ArraySpatialIndex(List<LabeledVector<L>> data, DistanceMeasure distanceMeasure) {
        this.data = Collections.unmodifiableList(data);
        this.distanceMeasure = distanceMeasure;
    }

    /** {@inheritDoc} */
    @Override public List<LabeledVector<L>> findKClosest(int k, Vector pnt) {
        if (k <= 0)
            throw new IllegalArgumentException("Number of neighbours should be positive.");

        Queue<PointWithDistance<L>> heap = new PriorityQueue<>(Collections.reverseOrder());

        for (LabeledVector<L> dataPnt : data) {
            double distance = distanceMeasure.compute(pnt, dataPnt.features());
            tryToAddIntoHeap(heap, k, dataPnt, distance);
        }

        return transformToListOrdered(heap);
    }
}
