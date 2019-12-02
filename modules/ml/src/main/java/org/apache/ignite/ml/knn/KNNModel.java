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

package org.apache.ignite.ml.knn;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.utils.PointWithDistance;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.transformToListOrdered;
import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.tryToAddIntoHeap;

/**
 * KNN model build on top of distributed spatial indices. Be aware that this model is linked with cluster environment
 * it's been built on and can't be saved or used in other places. Under the hood it keeps {@link Dataset} that consists
 * of a set of resources allocated across the cluster.
 *
 * @param <L> Label type.
 */
public abstract class KNNModel<L> implements IgniteModel<Vector, L>, SpatialIndex<L> {
    /** Dataset with {@link SpatialIndex} as a partition data. */
    private final Dataset<EmptyContext, SpatialIndex<L>> dataset;

    /** Distance measure. */
    protected final DistanceMeasure distanceMeasure;

    /** Number of neighbours. */
    protected final int k;

    /** Weighted or not. */
    protected final boolean weighted;

    /**
     * Constructs a new instance of KNN model.
     *
     * @param dataset Dataset with {@link SpatialIndex} as a partition data.
     * @param distanceMeasure Distance measure.
     * @param k Number of neighbours.
     * @param weighted Weighted or not.
     */
    protected KNNModel(Dataset<EmptyContext, SpatialIndex<L>> dataset, DistanceMeasure distanceMeasure, int k,
        boolean weighted) {
        this.dataset = dataset;
        this.distanceMeasure = distanceMeasure;
        this.k = k;
        this.weighted = weighted;
    }

    /** {@inheritDoc} */
    @Override public List<LabeledVector<L>> findKClosest(int k, Vector pnt) {
        List<LabeledVector<L>> res = dataset.compute(spatialIdx -> spatialIdx.findKClosest(k, pnt), (a, b) -> {
            Queue<PointWithDistance<L>> heap = new PriorityQueue<>(Collections.reverseOrder());
            tryToAddIntoHeap(heap, k, pnt, a, distanceMeasure);
            tryToAddIntoHeap(heap, k, pnt, b, distanceMeasure);
            return transformToListOrdered(heap);
        });

        return res == null ? Collections.emptyList() : res;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            dataset.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
