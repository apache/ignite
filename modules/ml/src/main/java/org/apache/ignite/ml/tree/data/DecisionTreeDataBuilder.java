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

package org.apache.ignite.ml.tree.data;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * A partition {@code data} builder that makes {@link DecisionTreeData}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 */
public class DecisionTreeDataBuilder<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, DecisionTreeData> {
    /** */
    private static final long serialVersionUID = 3678784980215216039L;

    /** Function that extracts features from an {@code upstream} data. */
    private final IgniteBiFunction<K, V, Vector> featureExtractor;

    /** Function that extracts labels from an {@code upstream} data. */
    private final IgniteBiFunction<K, V, Double> lbExtractor;

    /**
     * Constructs a new instance of decision tree data builder.
     *
     * @param featureExtractor Function that extracts features from an {@code upstream} data.
     * @param lbExtractor Function that extracts labels from an {@code upstream} data.
     */
    public DecisionTreeDataBuilder(IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
    }

    /** {@inheritDoc} */
    @Override public DecisionTreeData build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize, C ctx) {
        double[][] features = new double[Math.toIntExact(upstreamDataSize)][];
        double[] labels = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();

            features[ptr] = featureExtractor.apply(entry.getKey(), entry.getValue()).asArray();

            labels[ptr] = lbExtractor.apply(entry.getKey(), entry.getValue());

            ptr++;
        }

        return new DecisionTreeData(features, labels);
    }
}
