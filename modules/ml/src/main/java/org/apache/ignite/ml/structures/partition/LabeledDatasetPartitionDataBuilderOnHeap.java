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
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;

/**
 * Partition data builder that builds {@link LabeledVectorSet}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 */
public class LabeledDatasetPartitionDataBuilderOnHeap<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, LabeledVectorSet<LabeledVector>> {
    /** */
    private static final long serialVersionUID = -7820760153954269227L;

    /** Upstream vectorizer. */
    private final Preprocessor<K, V> preprocessor;

    /**
     * Constructs a new instance of SVM partition data builder.
     *
     * @param preprocessor Upstream preprocessor.
     */
    public LabeledDatasetPartitionDataBuilderOnHeap(Preprocessor<K, V> preprocessor) {
        this.preprocessor = preprocessor;
    }

    /** {@inheritDoc} */
    @Override public LabeledVectorSet<LabeledVector> build(
        LearningEnvironment env,
        Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize, C ctx) {
        int xCols = -1;
        double[][] x = null;
        double[] y = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;

        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();
            LabeledVector<Double> labeledVector = preprocessor.apply(entry.getKey(), entry.getValue());
            Vector row = labeledVector.features();

            if (xCols < 0) {
                xCols = row.size();
                x = new double[Math.toIntExact(upstreamDataSize)][xCols];
            }
            else
                assert row.size() == xCols : "X extractor must return exactly " + xCols + " columns";

            x[ptr] = row.asArray();

            y[ptr] = labeledVector.label();

            ptr++;
        }
        return new LabeledVectorSet<>(x, y);
    }
}
