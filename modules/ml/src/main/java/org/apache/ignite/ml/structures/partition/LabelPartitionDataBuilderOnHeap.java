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

/**
 * Partition data builder that builds {@link LabelPartitionDataOnHeap}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 */
public class LabelPartitionDataBuilderOnHeap<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, LabelPartitionDataOnHeap> {
    /** */
    private static final long serialVersionUID = -7820760153954269227L;

    /** Extractor of Y vector value. */
    private final IgniteBiFunction<K, V, Double> yExtractor;

    /**
     * Constructs a new instance of Label partition data builder.
     *
     * @param yExtractor Extractor of Y vector value.
     */
    public LabelPartitionDataBuilderOnHeap(IgniteBiFunction<K, V, Double> yExtractor) {
        this.yExtractor = yExtractor;
    }

    /** {@inheritDoc} */
    @Override public LabelPartitionDataOnHeap build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize,
                                        C ctx) {
        double[] y = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();

            y[ptr] = yExtractor.apply(entry.getKey(), entry.getValue());

            ptr++;
        }
        return new LabelPartitionDataOnHeap(y);
    }
}
