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

package org.apache.ignite.ml.dataset.primitive;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.tree.data.DecisionTreeData;

/**
 * A partition {@code data} builder that makes {@link DecisionTreeData}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 * @param <CO> Type of a coordinate for vectorizer.
 */
public class FeatureMatrixWithLabelsOnHeapDataBuilder<K, V, C extends Serializable, CO extends Serializable>
    implements PartitionDataBuilder<K, V, C, FeatureMatrixWithLabelsOnHeapData> {
    /** Serial version uid. */
    private static final long serialVersionUID = 6273736987424171813L;

    /** Function that extracts features and labels from an {@code upstream} data. */
    private final Preprocessor<K, V> preprocessor;

    /**
     * Constructs a new instance of decision tree data builder.
     *
     * @param preprocessor Function that extracts features with labels from an {@code upstream} data.
     */
    public FeatureMatrixWithLabelsOnHeapDataBuilder(Preprocessor<K, V> preprocessor) {
        this.preprocessor = preprocessor;
    }

    /** {@inheritDoc} */
    @Override public FeatureMatrixWithLabelsOnHeapData build(
        LearningEnvironment env,
        Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize,
        C ctx) {
        double[][] features = new double[Math.toIntExact(upstreamDataSize)][];
        double[] labels = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();

            LabeledVector<Double> labeledVector = preprocessor.apply(entry.getKey(), entry.getValue());
            features[ptr] = labeledVector.features().asArray();
            labels[ptr] = labeledVector.label();

            ptr++;
        }

        return new FeatureMatrixWithLabelsOnHeapData(features, labels);
    }
}
