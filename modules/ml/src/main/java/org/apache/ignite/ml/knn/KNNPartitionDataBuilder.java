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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.knn.utils.indices.ArraySpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.BallTreeSpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.KDTreeSpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndexType;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Partition data builder for KNN algorithms based on {@link SpatialIndex}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 */
public class KNNPartitionDataBuilder<K, V> implements PartitionDataBuilder<K, V, EmptyContext, SpatialIndex<Double>> {
    /** Data preprocessor. */
    private final Preprocessor<K, V> preprocessor;

    /** Spatial index type. */
    private final SpatialIndexType spatialIdxType;

    /** Distance measure. */
    private final DistanceMeasure distanceMeasure;

    /**
     * Constructs a new instance of KNN partition data builder.
     *
     * @param preprocessor Data preprocessor.
     * @param spatialIdxType Spatial index type.
     * @param distanceMeasure Distance measure.
     */
    public KNNPartitionDataBuilder(Preprocessor<K, V> preprocessor, SpatialIndexType spatialIdxType,
        DistanceMeasure distanceMeasure) {
        this.preprocessor = preprocessor;
        this.spatialIdxType = spatialIdxType;
        this.distanceMeasure = distanceMeasure;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public SpatialIndex<Double> build(LearningEnvironment env, Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize, EmptyContext ctx) {

        List<LabeledVector<Double>> dataPnts = new ArrayList<>();
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();
            dataPnts.add(preprocessor.apply(entry.getKey(), entry.getValue()));
        }

        switch (spatialIdxType) {
            case ARRAY: {
                return new ArraySpatialIndex<>(dataPnts, distanceMeasure);
            }
            case KD_TREE: {
                return new KDTreeSpatialIndex<>(dataPnts, distanceMeasure);
            }
            case BALL_TREE: {
                return new BallTreeSpatialIndex<>(dataPnts, distanceMeasure);
            }
            default:
                throw new IllegalArgumentException("Unknown spatial index type [type=" + spatialIdxType + "]");
        }
    }
}
