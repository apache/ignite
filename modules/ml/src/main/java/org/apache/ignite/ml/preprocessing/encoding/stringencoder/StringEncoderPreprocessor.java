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

import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownStringValue;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * Preprocessing function that makes String encoding.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class StringEncoderPreprocessor<K, V> implements IgniteBiFunction<K, V, Vector> {
    /** */
    private static final long serialVersionUID = 6237812226382623469L;
    /** */
    private static final String KEY_FOR_NULL_VALUES = "";

    /** Filling values. */
    private final Map<String, Integer>[] encodingValues;

    /** Base preprocessor. */
    private final IgniteBiFunction<K, V, Object[]> basePreprocessor;

    /** Feature indices to apply encoder.*/
    private final Set<Integer> handledIndices;

    /**
     * Constructs a new instance of String Encoder preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     * @param handledIndices Handled indices.
     */
    public StringEncoderPreprocessor(Map<String, Integer>[] encodingValues,
        IgniteBiFunction<K, V, Object[]> basePreprocessor, Set<Integer> handledIndices) {
        this.handledIndices = handledIndices;
        this.encodingValues = encodingValues;
        this.basePreprocessor = basePreprocessor;
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
        double[] res = new double[tmp.length];

        for (int i = 0; i < res.length; i++) {
            Object tmpObj = tmp[i];
            if(handledIndices.contains(i)){
                if(tmpObj.equals(Double.NaN) && encodingValues[i].containsKey(KEY_FOR_NULL_VALUES))
                    res[i] = encodingValues[i].get(KEY_FOR_NULL_VALUES);
                else if (encodingValues[i].containsKey(tmpObj))
                    res[i] = encodingValues[i].get(tmpObj);
                else
                    throw new UnknownStringValue(tmpObj.toString());
            } else
                res[i] = (double)tmpObj;
        }
        return VectorUtils.of(res);
    }
}
