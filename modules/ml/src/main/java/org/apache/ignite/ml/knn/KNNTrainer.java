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

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndexType;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * KNN model trader that trains model on top of distributed spatial indices. Be aware that this model is linked with
 * cluster environment it's been built on and can't be saved or used in other places. Under the hood it keeps
 * {@link Dataset} that consists of a set of resources allocated across the cluster.
 *
 * @param <M> Model type.
 */
public abstract class KNNTrainer<M extends KNNModel<Double>, Self extends KNNTrainer<M, Self>>
    extends SingleLabelDatasetTrainer<M> {
    /** Index type. */
    private SpatialIndexType idxType = SpatialIndexType.KD_TREE;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure = new EuclideanDistance();

    /** Number of neighbours. */
    protected int k = 5;

    /** Weighted or not. */
    protected boolean weighted;

    /** {@inheritDoc} */
    @Override protected <K, V> M fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        return updateModel(null, datasetBuilder, preprocessor);
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(M mdl) {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> M updateModel(M mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        Dataset<EmptyContext, SpatialIndex<Double>> knnDataset = datasetBuilder.build(
            envBuilder,
            new EmptyContextBuilder<>(),
            new KNNPartitionDataBuilder<>(preprocessor, idxType, distanceMeasure),
            learningEnvironment()
        );

        return convertDatasetIntoModel(knnDataset);
    }

    /**
     * Convers given dataset into KNN model (classification or regression depends on implementation).
     *
     * @param dataset Dataset of spatial indices.
     * @return KNN model.
     */
    protected abstract M convertDatasetIntoModel(Dataset<EmptyContext, SpatialIndex<Double>> dataset);

    /**
     * Returns {@code this} instance.
     *
     * @return This instance.
     */
    protected abstract Self self();

    /**
     * Sets up {@code idxType} parameter.
     *
     * @param idxType Index type.
     */
    public Self withIdxType(SpatialIndexType idxType) {
        this.idxType = idxType;
        return self();
    }

    /**
     * Sets up {@code dataTtl} parameter.
     *
     * @param dataTtl Partition data time-to-live in seconds (-1 for an infinite lifetime).
     * @return This instance.
     */
    public Self withDataTtl(long dataTtl) {
        envBuilder = envBuilder.withDataTtl(dataTtl);
        return self();
    }

    /**
     * Sets up {@code distanceMeasure} parameter.
     *
     * @param distanceMeasure Distance measure.
     */
    public Self withDistanceMeasure(DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
        return self();
    }

    /**
     * Sets up {@code k} parameter (number of neighbours).
     *
     * @param k Number of neighbours.
     */
    public Self withK(int k) {
        this.k = k;
        return self();
    }

    /**
     * Sets up {@code weighted} parameter.
     *
     * @param weighted Weighted or not.
     */
    public Self withWeighted(boolean weighted) {
        this.weighted = weighted;
        return self();
    }
}
