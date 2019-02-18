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

package org.apache.ignite.ml.trainers;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Class fro extracting features and vectors from upstream.
 *
 * @param <K> Type of keys.
 * @param <V> Type of values.
 * @param <L> Type of labels.
 */
public interface FeatureLabelExtractor<K, V, L> extends Serializable {
    /**
     * Extract {@link LabeledVector} from key and value.
     *
     * @param k Key.
     * @param v Value.
     * @return Labeled vector.
     */
    public LabeledVector<L> extract(K k, V v);

    /** */
    public default <L1> FeatureLabelExtractor<K, V, L1> andThen(IgniteFunction<? super LabeledVector<L>, ? extends LabeledVector<L1>> after) {
        Objects.requireNonNull(after);
        return (K k, V v) -> after.apply(extract(k, v));
    }

    /**
     * Extract features from key and value.
     *
     * @param key Key.
     * @param val Value.
     * @return Features vector.
     */
    public default Vector extractFeatures(K key, V val) {
        return extract(key, val).features();
    }

    /**
     * Extract label from key and value.
     *
     * @param key Key.
     * @param val Value.
     * @return Label.
     */
    public default L extractLabel(K key, V val) {
        return extract(key, val).label();
    }
}
