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

package org.apache.ignite.ml.dlc.dataset.transformer;

import java.io.Serializable;
import org.apache.ignite.ml.dlc.DLCPartitionRecoverableTransformer;
import org.apache.ignite.ml.dlc.DLCUpstreamEntry;
import org.apache.ignite.ml.dlc.dataset.DLCLabeledDataset;
import org.apache.ignite.ml.dlc.dataset.part.DLCLabeledDatasetPartitionRecoverable;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Transforms upstream data into the {@link DLCLabeledDataset} using the specified feature and label extractors.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class UpstreamToLabeledDatasetTransformer<K, V, Q extends Serializable>
    implements DLCPartitionRecoverableTransformer<K, V, Q, DLCLabeledDatasetPartitionRecoverable> {
    /** */
    private static final long serialVersionUID = -1224768715207401297L;

    /** Feature extractor. */
    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    /** Label extractor. */
    private final IgniteBiFunction<K, V, Double> lbExtractor;

    /** Number of features. */
    private final int features;

    /**
     * Constructs a new instance of transformer.
     *
     * @param featureExtractor feature extractor
     * @param lbExtractor label extractor
     * @param features number of features
     */
    public UpstreamToLabeledDatasetTransformer(
        IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor, int features) {
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.features = features;
    }

    /**
     * Transforms upstream data to {@link DLCLabeledDatasetPartitionRecoverable}.
     *
     * @param upstreamData upstream data
     * @param upstreamDataSize upstream data size
     * @param replicatedData replicated data
     * @return labeled dataset recoverable data
     */
    @Override public DLCLabeledDatasetPartitionRecoverable transform(Iterable<DLCUpstreamEntry<K, V>> upstreamData,
        Long upstreamDataSize, Q replicatedData) {
        int rows = Math.toIntExact(upstreamDataSize), cols = features;

        double[] features = new double[rows * cols];
        double[] labels = new double[rows];

        int ptr = 0;
        for (DLCUpstreamEntry<K, V> e : upstreamData) {
            double[] row = featureExtractor.apply(e.getKey(), e.getValue());

            assert cols == row.length;

            for (int i = 0; i < cols; i++)
                features[i * rows + ptr] = row[i];

            labels[ptr] = lbExtractor.apply(e.getKey(), e.getValue());

            ptr++;
        }

        return new DLCLabeledDatasetPartitionRecoverable(features, rows, cols, labels);
    }
}
