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

package org.apache.ignite.ml.structures.partition;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Partition data builder that builds {@link LabeledDataset}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 */
public class LabeledDatasetPartitionDataBuilderOnHeap<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, LabeledDataset<Double, LabeledVector>> {
    /** */
    private static final long serialVersionUID = -7820760153954269227L;

    /** Extractor of X matrix row. */
    private final IgniteBiFunction<K, V, Vector> xExtractor;

    /** Extractor of Y vector value. */
    private final IgniteBiFunction<K, V, Double> yExtractor;

    /**
     * Constructs a new instance of SVM partition data builder.
     *
     * @param xExtractor Extractor of X matrix row.
     * @param yExtractor Extractor of Y vector value.
     */
    public LabeledDatasetPartitionDataBuilderOnHeap(IgniteBiFunction<K, V, Vector> xExtractor,
                                         IgniteBiFunction<K, V, Double> yExtractor) {
        this.xExtractor = xExtractor;
        this.yExtractor = yExtractor;
    }

    /** {@inheritDoc} */
    @Override public LabeledDataset<Double, LabeledVector> build(Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize, C ctx) {
        int xCols = -1;
        double[][] x = null;
        double[] y = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;

        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();
            Vector row = xExtractor.apply(entry.getKey(), entry.getValue());

            if (xCols < 0) {
                xCols = row.size();
                x = new double[Math.toIntExact(upstreamDataSize)][xCols];
            }
            else
                assert row.size() == xCols : "X extractor must return exactly " + xCols + " columns";

            x[ptr] = row.asArray();

            y[ptr] = yExtractor.apply(entry.getKey(), entry.getValue());

            ptr++;
        }
        return new LabeledDataset<>(x, y);
    }
}
