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

package org.apache.ignite.ml.preprocessing.minmaxscaling;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Preprocessing function that makes minmaxscaling. From mathematical point of view it's the following function which
 * is applied to every element in dataset:
 *
 * {@code a_i = (a_i - min_i) / (max_i - min_i) for all i},
 *
 * where {@code i} is a number of column, {@code max_i} is the value of the maximum element in this columns,
 * {@code min_i} is the value of the minimal element in this column.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public final class MinMaxScalerPreprocessor<K, V> implements Preprocessor<K, V>, DeployableObject {
    /** */
    private static final long serialVersionUID = 6997800576392623469L;

    /** Minimal values. */
    private final double[] min;

    /** Maximum values. */
    private final double[] max;

    /** Base preprocessor. */
    private final Preprocessor<K, V> basePreprocessor;

    /**
     * Constructs a new instance of minmaxscaling preprocessor.
     *
     * @param min Minimal values.
     * @param max Maximum values.
     * @param basePreprocessor Base preprocessor.
     */
    public MinMaxScalerPreprocessor(double[] min, double[] max, Preprocessor<K, V> basePreprocessor) {
        this.min = min;
        this.max = max;
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

        assert res.size() == min.length;
        assert res.size() == max.length;

        for (int i = 0; i < res.size(); i++) {
            double num = res.get(i) - min[i];
            double denom = max[i] - min[i];
            double scaled = num / denom;

            if (Double.isNaN(scaled))
                res.set(i, num);
            else
                res.set(i, scaled);
        }

        return res;
    }

    /** */
    public double[] getMin() {
        return min;
    }

    /** */
    public double[] getMax() {
        return max;
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(basePreprocessor);
    }
}
