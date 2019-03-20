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

package org.apache.ignite.ml.dataset.feature.extractor.impl;

import java.util.List;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

/**
 * Temporary class for Features/Label extracting.
 */
public class FeatureLabelExtractorWrapper<K, V, L> extends Vectorizer<K, V, Object, L> {
    /** Original extractor. */
    private final FeatureLabelExtractor<K, V, L> extractor;

    /**
     * Creates an instance of FeatureLabelExtractorWrapper.
     *
     * @param extractor Features and lavels extractor.
     */
    public FeatureLabelExtractorWrapper(FeatureLabelExtractor<K, V, L> extractor) {
        this.extractor = extractor;
    }

    /**
     * @param featuresEx Method for feature vector extracting.
     * @return wrapper.
     */
    public static <K, V> FeatureLabelExtractorWrapper<K, V, Double> wrap(IgniteBiFunction<K, V, Vector> featuresEx) {
        return new FeatureLabelExtractorWrapper<>((k, v) -> featuresEx.apply(k, v).labeled(0.0));
    }

    public static <K, V, L> FeatureLabelExtractorWrapper<K, V, L> wrap(IgniteBiFunction<K, V, Vector> featuresEx,
        IgniteBiFunction<K, V, L> lbExtractor) {

        return new FeatureLabelExtractorWrapper<>(CompositionUtils.asFeatureLabelExtractor(featuresEx, lbExtractor));
    }

    /** {@inheritDoc} */
    @Override public LabeledVector<L> apply(K key, V value) {
        return extractor.extract(key, value);
    }

    /** {@inheritDoc} */
    @Override protected Double feature(Object coord, K key, V value) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override protected L label(Object coord, K key, V value) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override protected L zero() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override protected List<Object> allCoords(K key, V value) {
        throw new IllegalStateException();
    }
}
