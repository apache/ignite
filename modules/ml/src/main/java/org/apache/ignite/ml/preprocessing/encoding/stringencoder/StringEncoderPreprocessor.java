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

package org.apache.ignite.ml.preprocessing.encoding.stringencoder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.math.exceptions.preprocessing.IllegalFeatureTypeException;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownCategorialValueException;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Preprocessing function that makes String encoding.
 *
 * The String Encoder Preprocessor encodes string values (categories) to double values
 * in range [0.0, amountOfCategories), where the most popular value will be presented as 0.0 and
 * the least popular value presented with amountOfCategories-1 value.
 * <p>
 * This preprocessor can transform multiple columns which indices are handled during training process.
 * These indexes could be defined via .withEncodedFeature(featureIndex) call.
 * </p>
 * <p>
 * NOTE: it does not add new column but change data in-place.
 *</p>
 * <p>
 * There is only a one strategy regarding how StringEncoder will handle unseen labels
 * when you have fit a StringEncoder on one dataset and then use it to transform another:
 * put unseen labels in a special additional bucket, at index is equal amountOfCategories.
 * </p>
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public final class StringEncoderPreprocessor<K, V> extends EncoderPreprocessor<K, V> implements DeployableObject {
    /** */
    private static final long serialVersionUID = 6237712226382623488L;

    /**
     * Constructs a new instance of String Encoder preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     * @param handledIndices   Handled indices.
     */
    public StringEncoderPreprocessor(Map<String, Integer>[] encodingValues,
                                     Preprocessor<K, V> basePreprocessor, Set<Integer> handledIndices) {
        super(encodingValues, basePreprocessor, handledIndices);
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
            Object tmpObj = tmp.getRaw(i);
            if (handledIndices.contains(i)) {
                if (tmpObj.equals(Double.NaN) && encodingValues[i].containsKey(KEY_FOR_NULL_VALUES))
                    res[i] = encodingValues[i].get(KEY_FOR_NULL_VALUES);
                else if (encodingValues[i].containsKey(tmpObj))
                    res[i] = encodingValues[i].get(tmpObj);
                else
                    throw new UnknownCategorialValueException(tmpObj.toString());
            } else {
                if (tmpObj instanceof Number)
                    res[i] = (double)tmpObj;
                else
                    throw new IllegalFeatureTypeException(tmpObj.getClass(), tmpObj, Double.class);
            }

        }
        return new LabeledVector(VectorUtils.of(res), tmp.label());
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(basePreprocessor);
    }
}
