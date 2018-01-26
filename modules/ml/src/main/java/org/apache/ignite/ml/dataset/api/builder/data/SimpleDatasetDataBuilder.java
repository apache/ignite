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

package org.apache.ignite.ml.dataset.api.builder.data;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.PartitionUpstreamEntry;
import org.apache.ignite.ml.dataset.api.data.SimpleDatasetData;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

public class SimpleDatasetDataBuilder<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, SimpleDatasetData> {

    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    private final int cols;

    public SimpleDatasetDataBuilder(IgniteBiFunction<K, V, double[]> featureExtractor, int cols) {
        this.featureExtractor = featureExtractor;
        this.cols = cols;
    }

    /** */
    @Override public SimpleDatasetData build(Iterator<PartitionUpstreamEntry<K, V>> upstreamData, long upstreamDataSize,
        C ctx) {
        double[] features = new double[Math.toIntExact(upstreamDataSize * cols)];

        int ptr = 0;
        while (upstreamData.hasNext()) {
            PartitionUpstreamEntry<K, V> entry = upstreamData.next();
            double[] row = featureExtractor.apply(entry.getKey(), entry.getValue());

            assert row.length == cols;

            for (int i = 0; i < cols; i++)
                features[Math.toIntExact(i * upstreamDataSize) + ptr] = row[i];

            ptr++;
        }

        return new SimpleDatasetData(features, Math.toIntExact(upstreamDataSize), cols);
    }
}
