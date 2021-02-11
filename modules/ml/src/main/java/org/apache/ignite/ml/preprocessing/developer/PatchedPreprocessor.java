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
 * Preprocessing function that makes binarization.
 *
 * Feature values greater than the threshold are binarized to 1.0;
 * values equal to or less than the threshold are binarized to 0.0.
 *
 * NOTE: This is a part of Developer API for internal needs.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class PatchedPreprocessor<K, V, L1, L2> implements Preprocessor<K, V>, DeployableObject {
    /** */
    private static final long serialVersionUID = 6865823577892621239L;

    /** Base preprocessor. */
    private final Preprocessor<K, V> basePreprocessor;

    /** Label patcher. */
    private final IgniteFunction<LabeledVector<L1>, LabeledVector<L2>> lbPatcher;

    /**
     * Constructs a new instance of Binarization preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     */
    public PatchedPreprocessor(IgniteFunction<LabeledVector<L1>, LabeledVector<L2>> lbPatcher, Preprocessor<K, V> basePreprocessor) {
        this.lbPatcher = lbPatcher;
        this.basePreprocessor = basePreprocessor;
    }

    /**
     * Applies this preprocessor.
     *
     * @param k Key.
     * @param v Value.
     * @return Preprocessed row.
     */
    @Override public LabeledVector apply(K k, V v) {
        LabeledVector<L1> tmp = basePreprocessor.apply(k, v);

        return lbPatcher.apply(tmp);
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(basePreprocessor);
    }
}
