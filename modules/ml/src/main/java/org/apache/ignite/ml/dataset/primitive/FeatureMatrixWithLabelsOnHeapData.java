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

package org.apache.ignite.ml.dataset.primitive;

/**
 * A partition {@code data} of the containing matrix of features and vector of labels stored in heap.
 */
public class FeatureMatrixWithLabelsOnHeapData implements AutoCloseable {
    /** Matrix with features. */
    private final double[][] features;

    /** Vector with labels. */
    private final double[] labels;

    /**
     * Constructs an instance of FeatureMatrixWithLabelsOnHeapData.
     *
     * @param features Features.
     * @param labels Labels.
     */
    public FeatureMatrixWithLabelsOnHeapData(double[][] features, double[] labels) {
        assert features.length == labels.length : "Features and labels have to be the same length";

        this.features = features;
        this.labels = labels;
    }

    /** */
    public double[][] getFeatures() {
        return features;
    }

    /** */
    public double[] getLabels() {
        return labels;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
