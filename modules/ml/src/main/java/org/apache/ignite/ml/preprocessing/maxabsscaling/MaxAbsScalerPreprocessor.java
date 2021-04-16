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

package org.apache.ignite.ml.preprocessing.maxabsscaling;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * The preprocessing function that makes maxabsscaling, transforms features to the scale {@code [-1,+1]}. From
 * mathematical point of view it's the following function which is applied to every element in a dataset:
 *
 * {@code a_i = a_i / maxabs_i for all i},
 *
 * where {@code i} is a number of column, {@code maxabs_i} is the value of the absolute maximum element in this column.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public final class MaxAbsScalerPreprocessor<K, V> implements Preprocessor<K, V>, DeployableObject {
    /** */
    private static final long serialVersionUID = 1L;

    /** Maximum absolute values. */
    private final double[] maxAbs;

    /** Base preprocessor. */
    private final Preprocessor<K, V> basePreprocessor;

    /**
     * Constructs a new instance of maxabsscaling preprocessor.
     *
     * @param maxAbs Maximal absolute values.
     * @param basePreprocessor Base preprocessor.
     */
    public MaxAbsScalerPreprocessor(double[] maxAbs, Preprocessor<K, V> basePreprocessor) {
        this.maxAbs = maxAbs;
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
        LabeledVector res = basePreprocessor.apply(k, v);

        assert res.size() == maxAbs.length;

        for (int i = 0; i < res.size(); i++)
            res.set(i, res.get(i) / maxAbs[i]);

        return res;
    }

    /** */
    public double[] getMaxAbs() {
        return maxAbs;
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(basePreprocessor);
    }
}
