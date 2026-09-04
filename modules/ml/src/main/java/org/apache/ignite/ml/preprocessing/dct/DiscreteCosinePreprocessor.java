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

package org.apache.ignite.ml.preprocessing.dct;

import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Preprocessing function that applies discrete cosine transformation.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class DiscreteCosinePreprocessor<K, V> implements Preprocessor<K, V> {
    /** */
    private static final long serialVersionUID = 6877811577892627461L;

    /** Base preprocessor */
    private final Preprocessor<K, V> basePreprocessor;

    /** DCT type, default is 2 */
    private final int type;

    /**
     * Constructs a new instance of Discrete Cosine preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     * @param type DCT type.
     */
    public DiscreteCosinePreprocessor(Preprocessor<K, V> basePreprocessor, int type) {
        this.basePreprocessor = basePreprocessor;
        this.type = type;
    }

    /**
     * Applies this preprocessor.
     *
     * @param k Key.
     * @param v Value.
     * @return Preprocessed row.
     */
    @Override public LabeledVector apply(K k, V v) {
        LabeledVector tmp = basePreprocessor.apply(k, v);
        double[] res = new double[tmp.size()];

        for (int i = 0; i < res.length; i++) {
            double sum = 0;

            if (type == 1) {
                sum += tmp.get(0) + Math.pow(-1, i) * tmp.get(tmp.size() - 1);

                for (int j = 1; j < tmp.size() - 1; j++)
                    sum += 2 * tmp.get(j) * Math.cos((Math.PI / (tmp.size() - 1)) * j * i);
            }
            else if (type == 2) {
                for (int j = 0; j < tmp.size(); j++)
                    sum += 2 * tmp.get(j) * Math.cos((Math.PI / tmp.size()) * (j + 0.5) * i);
            }
            else if (type == 3) {
                sum += tmp.get(0);

                for (int j = 1; j < tmp.size(); j++)
                    sum += 2 * tmp.get(j) * Math.cos((Math.PI / tmp.size()) * j * (i + 0.5));
            }
            else if (type == 4) {
                for (int j = 0; j < tmp.size(); j++)
                    sum += 2 * tmp.get(j) * Math.cos((Math.PI / tmp.size()) * (j + 0.5) * (i + 0.5));
            }
            else
                throw new IllegalArgumentException("Invalid type: " + type);

            res[i] = sum;
        }

        return new LabeledVector<>(VectorUtils.of(res), tmp.label());
    }
}
