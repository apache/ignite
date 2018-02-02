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

package org.apache.ignite.ml.dataset.primitive.builder.data;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * A partition {@code data} builder that makes {@link SimpleLabeledDatasetData}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> type of a partition <tt>context</tt>.
 */
public class SimpleLabeledDatasetDataBuilder<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, SimpleLabeledDatasetData> {
    /** */
    private static final long serialVersionUID = 3678784980215216039L;

    /** Function that extracts features from an {@code upstream} data. */
    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    /** Function that extracts labels from an {@code upstream} data. */
    private final IgniteBiFunction<K, V, Double> lbExtractor;

    /** Number of columns (features). */
    private final int cols;

    /**
     * Constructs a new instance of partition {@code data} builder that makes {@link SimpleLabeledDatasetData}.
     *
     * @param featureExtractor Function that extracts features from an {@code upstream} data.
     * @param lbExtractor Function that extracts labels from an {@code upstream} data.
     * @param cols Number of columns (features).
     */
    public SimpleLabeledDatasetDataBuilder(IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor, int cols) {
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.cols = cols;
    }

    /** {@inheritDoc} */
    @Override public SimpleLabeledDatasetData build(Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize, C ctx) {
        // Prepares the matrix of features in flat column-major format.
        double[] features = new double[Math.toIntExact(upstreamDataSize * cols)];
        double[] labels = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();
            double[] row = featureExtractor.apply(entry.getKey(), entry.getValue());

            assert row.length == cols : "Feature extractor must return exactly " + cols + " features";

            for (int i = 0; i < cols; i++)
                features[Math.toIntExact(i * upstreamDataSize) + ptr] = row[i];

            labels[ptr] = lbExtractor.apply(entry.getKey(), entry.getValue());

            ptr++;
        }

        return new SimpleLabeledDatasetData(features, Math.toIntExact(upstreamDataSize), cols, labels);
    }
}
