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

package org.apache.ignite.ml.preprocessing.encoding.onehotencoder;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownCategorialFeatureValue;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.encoding.EncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;

/**
 * Preprocessing function that makes one-hot encoding.
 *
 * One-hot encoding maps a categorical feature,
 * represented as a label index (Double or String value),
 * to a binary vector with at most a single one-value indicating the presence of a specific feature value
 * from among the set of all feature values.
 *
 * This preprocessor can transform multiple columns which indices are handled during training process.
 *
 * Each one-hot encoded binary vector adds its cells to the end of the current feature vector.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 * @see EncoderTrainer
 *
 * This prerpocessor always creates separate column for the NULL values.
 *
 * NOTE: the index value associated with NULL will located in binary vector according the frequency of NULL values.
 */
public class OneHotEncoderPreprocessor<K, V> extends EncoderPreprocessor<K, V> {
    /** */
    private static final long serialVersionUID = 6237812226552623469L;

    /**
     * Constructs a new instance of One Hot Encoder preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     * @param handledIndices   Handled indices.
     */
    public OneHotEncoderPreprocessor(Map<String, Integer>[] encodingValues,
                                     IgniteBiFunction<K, V, Object[]> basePreprocessor, Set<Integer> handledIndices) {
        super(encodingValues, basePreprocessor, handledIndices);
    }

    /**
     * Applies this preprocessor.
     *
     * @param k Key.
     * @param v Value.
     * @return Preprocessed row.
     */
    @Override public Vector apply(K k, V v) {
        Object[] tmp = basePreprocessor.apply(k, v);

        double[] res = new double[tmp.length + getAdditionalSize(encodingValues)];

        int categorialFeatureCntr = 0;

        for (int i = 0; i < tmp.length; i++) {
            Object tmpObj = tmp[i];
            if (handledIndices.contains(i)) {
                categorialFeatureCntr++;

                if (tmpObj.equals(Double.NaN) && encodingValues[i].containsKey(KEY_FOR_NULL_VALUES)) {
                    final Integer indexedVal = encodingValues[i].get(KEY_FOR_NULL_VALUES);

                    res[i] = indexedVal;

                    res[tmp.length + getIdxOffset(categorialFeatureCntr, indexedVal, encodingValues)] = 1.0;
                } else {
                    final String key = String.valueOf(tmpObj);

                    if (encodingValues[i].containsKey(key)) {
                        final Integer indexedVal = encodingValues[i].get(key);

                        res[i] = indexedVal;

                        res[tmp.length + getIdxOffset(categorialFeatureCntr, indexedVal, encodingValues)] = 1.0;

                    } else
                        throw new UnknownCategorialFeatureValue(tmpObj.toString());
                }

            } else
                res[i] = (double) tmpObj;
        }
        return VectorUtils.of(res);
    }

    /**
     * Calculates the additional size of feature vector based on trainer's stats.
     * It adds amount of column for each categorial feature equal to amount of categories.
     *
     * @param encodingValues The given trainer stats which helps to calculates the actual size of feature vector.
     * @return The additional size.
     */
    private int getAdditionalSize(Map<String, Integer>[] encodingValues) {
        int newSize = 0;
        for (Map<String, Integer> encodingValue : encodingValues) {
            if (encodingValue != null)
                newSize += encodingValue.size(); // - 1 if we don't keep NULL values and it has NULL values
        }
        return newSize;
    }

    /**
     * Calculates the offset in feature vector to set up 1.0 accordingly the index value.
     *
     * @param categorialFeatureCntr The actual order number for the current categorial feature.
     * @param indexedVal            The indexed value, converted from the raw value.
     * @param encodingValues        The trainer's stats about category frequencies.
     * @return The offset.
     */
    private int getIdxOffset(int categorialFeatureCntr, int indexedVal, Map<String, Integer>[] encodingValues) {
        int idxOff = 0;

        int locCategorialFeatureCntr = 1;

        for (int i = 0; locCategorialFeatureCntr < categorialFeatureCntr; i++) {
            if (encodingValues[i] != null) {
                locCategorialFeatureCntr++;
                idxOff += encodingValues[i].size();  // - 1 if we don't keep NULL values and it has NULL values
            }
        }

        idxOff += indexedVal;

        return idxOff;
    }
}
