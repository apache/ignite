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
import org.apache.ignite.ml.dataset.primitive.data.SimpleDatasetData;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * A partition {@code data} builder that makes {@link SimpleDatasetData}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 */
public class SimpleDatasetDataBuilder<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, SimpleDatasetData> {
    /** */
    private static final long serialVersionUID = 756800193212149975L;

    /** Function that extracts features from an {@code upstream} data. */
    private final IgniteBiFunction<K, V, Vector> featureExtractor;

    /**
     * Construct a new instance of partition {@code data} builder that makes {@link SimpleDatasetData}.
     *
     * @param featureExtractor Function that extracts features from an {@code upstream} data.
     */
    public SimpleDatasetDataBuilder(IgniteBiFunction<K, V, Vector> featureExtractor) {
        this.featureExtractor = featureExtractor;
    }

    /** {@inheritDoc} */
    @Override public SimpleDatasetData build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize, C ctx) {
        // Prepares the matrix of features in flat column-major format.
        int cols = -1;
        double[] features = null;

        int ptr = 0;
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();
            Vector row = featureExtractor.apply(entry.getKey(), entry.getValue());

            if (cols < 0) {
                cols = row.size();
                features = new double[Math.toIntExact(upstreamDataSize * cols)];
            }
            else
                assert row.size() == cols : "Feature extractor must return exactly " + cols + " features";

            for (int i = 0; i < cols; i++)
                features[Math.toIntExact(i * upstreamDataSize + ptr)] = row.get(i);

            ptr++;
        }

        return new SimpleDatasetData(features, Math.toIntExact(upstreamDataSize));
    }
}
