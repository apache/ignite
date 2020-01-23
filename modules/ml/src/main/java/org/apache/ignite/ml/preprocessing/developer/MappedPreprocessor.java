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

package org.apache.ignite.ml.preprocessing.developer;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Mapped Preprocessor.
 *
 * @param <K> Type of key.
 * @param <V> Type of value.
 * @param <L0> Type of original label.
 * @param <L1> Type of mapped label.
 */
public final class MappedPreprocessor<K, V, L0, L1> implements Preprocessor<K, V>, DeployableObject {
    /** Original preprocessor. */
    private final Preprocessor<K, V> original;

    /** Vectors mapping. */
    private final IgniteFunction<LabeledVector<L0>, LabeledVector<L1>> mapping;

    /**
     * Creates an instance of MappedPreprocessor.
     */
    public MappedPreprocessor(Preprocessor<K, V> original,
        IgniteFunction<LabeledVector<L0>, LabeledVector<L1>> andThen) {

        this.original = original;
        this.mapping = andThen;
    }

    /** {@inheritDoc} */
    @Override public LabeledVector<L1> apply(K key, V value) {
        LabeledVector<L0> origVec = original.apply(key, value);
        return mapping.apply(origVec);
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(original);
    }
}
