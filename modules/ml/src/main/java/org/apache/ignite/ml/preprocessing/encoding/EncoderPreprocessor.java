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

package org.apache.ignite.ml.preprocessing.encoding;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.preprocessing.Preprocessor;

/**
 * Preprocessing function that makes encoding.
 *
 * This a base abstract class that keeps the common fields for all child encoding preprocessors.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public abstract class EncoderPreprocessor<K, V> implements Preprocessor<K, V> {
    /** */
    public static final String KEY_FOR_NULL_VALUES = "";

    /** Filling values. */
    protected Map<String, Integer>[] encodingValues;

    /** Frequencies of categories for label presented as strings. */
    protected Map<String, Integer> labelFrequencies;

    /** Base preprocessor. */
    protected final Preprocessor<K, V> basePreprocessor;

    /** Feature indices to apply encoder. */
    protected Set<Integer> handledIndices;

    /**
     * Constructs a new instance of Encoder preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     * @param handledIndices Handled indices.
     */
    protected EncoderPreprocessor(Map<String, Integer>[] encodingValues,
        Preprocessor<K, V> basePreprocessor, Set<Integer> handledIndices) {
        this.handledIndices = handledIndices;
        this.encodingValues = encodingValues;
        this.basePreprocessor = basePreprocessor;
    }

    /**
     * Constructs a new instance of Encoder preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     */
    protected EncoderPreprocessor(Map<String, Integer> labelFrequencies,
        Preprocessor<K, V> basePreprocessor) {
        this.labelFrequencies = labelFrequencies;
        this.basePreprocessor = basePreprocessor;
    }
}
