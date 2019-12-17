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

package org.apache.ignite.ml.preprocessing.encoding.label;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownCategorialValueException;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Preprocessing function that makes Label encoding.
 *
 * The Label Encoder Preprocessor encodes string values (categories) to double values
 * in range [0.0, amountOfCategories), where the most popular value will be presented as 0.0 and
 * the least popular value presented with amountOfCategories-1 value.
 * <p>
 * This preprocessor can transforms label column.
 * </p>
 * <p>
 * NOTE: it does not add new column but change data in-place.
 *</p>
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public final class LabelEncoderPreprocessor<K, V> extends EncoderPreprocessor<K, V> implements DeployableObject {
    /** */
    private static final long serialVersionUID = 6237782236382623488L;

    /**
     * Constructs a new instance of Label Encoder preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     */
    public LabelEncoderPreprocessor(Map<String, Integer> labelFrequencies,
                                     Preprocessor<K, V> basePreprocessor) {
        super(labelFrequencies, basePreprocessor);
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
        double res;
        Object tmpObj = tmp.label();

        if (tmpObj.equals(Double.NaN) && labelFrequencies.containsKey(KEY_FOR_NULL_VALUES))
            res = labelFrequencies.get(KEY_FOR_NULL_VALUES);
        else if (labelFrequencies.containsKey(tmpObj))
            res = labelFrequencies.get(tmpObj);
        else
            throw new UnknownCategorialValueException(tmpObj.toString());

        return new LabeledVector(tmp.features(), res);
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(basePreprocessor);
    }
}
