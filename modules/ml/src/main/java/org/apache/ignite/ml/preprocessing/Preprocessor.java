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

package org.apache.ignite.ml.preprocessing;

import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.preprocessing.developer.MappedPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Basic interface in Preprocessor Hierarchy.
 * @param <K>
 * @param <V>
 */
public interface Preprocessor<K, V> extends IgniteBiFunction<K, V, LabeledVector> {
    /**
     * Map vectorizer answer. This method should be called after creating basic vectorizer.
     * NOTE: function "func" should be on ignite servers.
     *
     * @param func mapper.
     * @param <L1> Type of new label.
     * @return mapped vectorizer.
     */
    public default <L1, L2> Preprocessor<K, V> map(IgniteFunction<LabeledVector<L1>, LabeledVector<L2>> func) {
        return new MappedPreprocessor<>(this, func);
    }
}
