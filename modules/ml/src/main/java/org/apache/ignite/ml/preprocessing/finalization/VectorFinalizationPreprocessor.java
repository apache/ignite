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

package org.apache.ignite.ml.preprocessing.finalization;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Preprocessing function that makes vector finalization.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public final class VectorFinalizationPreprocessor<K, V> implements Preprocessor<K, V>, DeployableObject {
    /** */
    private static final long serialVersionUID = 6877811575592627239L;

    /** Base preprocessor. */
    private final Preprocessor<K, V> basePreprocessor;

    /**
     * Constructs a new instance of Binarization preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     */
    public VectorFinalizationPreprocessor(Preprocessor<K, V> basePreprocessor) {
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
        NumberFormat format = NumberFormat.getInstance(Locale.FRANCE);

        for (int i = 0; i < res.size(); i++) {
            // if(res.get(i) instanceof Double)

            // Double convertedValue =

            //   res.set(i, "".equals(res[i]) ? Double.NaN : Double.valueOf(cells[i])); =

        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(basePreprocessor);
    }
}
