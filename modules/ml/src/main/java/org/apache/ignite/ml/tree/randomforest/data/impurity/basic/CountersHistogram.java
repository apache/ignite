/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.tree.randomforest.data.impurity.basic;

import java.util.Set;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;

/**
 * Represents a historam of element counts per bucket.
 */
public class CountersHistogram extends BootstrappedVectorsHistogram {
    /** Serial version uid. */
    private static final long serialVersionUID = 7744564790854918891L;

    /** Sample id. */
    private final int sampleId;

    /**
     * Creates an instance of CountersHistogram.
     *
     * @param bucketIds Bucket ids.
     * @param bucketMeta Bucket meta.
     * @param featureId Feature id.
     * @param sampleId Sample Id.
     */
    public CountersHistogram(Set<Integer> bucketIds, BucketMeta bucketMeta, int featureId, int sampleId) {
        super(bucketIds, bucketMeta, featureId);
        this.sampleId = sampleId;
    }

    /** {@inheritDoc} */
    @Override public Double mapToCounter(BootstrappedVector vec) {
        return (double)vec.counters()[sampleId];
    }

    /** {@inheritDoc} */
    @Override public ObjectHistogram<BootstrappedVector> newInstance() {
        return new CountersHistogram(bucketIds, bucketMeta, featureId, sampleId);
    }
}
