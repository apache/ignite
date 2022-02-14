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

package org.apache.ignite.ml.preprocessing.encoding.target;

import java.util.Set;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Preprocessing function that makes Target encoding.
 *
 * The Target Encoder Preprocessor encodes string values (categories) to double values
 * in range [0.0, 1], where the value will be presented as a regularized mean target value of
 * a category.
 *
 * alpha = 1 / (1 + exp(-(categorySize - min_samples_leaf) / beta))
 * encodedValue = globalTargetMean * (1 - alpha) + categoryTargetMean * alpha
 * if categorySize == 1 then use globalTargetMean
 *
 * min_samples_leaf - minimum samples to take category average into account.
 * beta - smoothing effect to balance categorical average vs prior. Higher value means
 * stronger regularization.
 *
 * ref: https://dl.acm.org/doi/10.1145/507533.507538
 *
 * <p>
 * This preprocessor can transform multiple columns which indices are handled during training process.
 * These indexes could be defined via .withEncodedFeature(featureIndex) call.
 * </p>
 * <p>
 * NOTE: it does not add new column but change data in-place.
 * </p>
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class TargetEncoderPreprocessor<K, V> extends EncoderPreprocessor<K, V> {
    /** */
    protected static final long serialVersionUID = 6237711236382623481L;

    /** Filling values. */
    protected final TargetEncodingMeta[] targetCounters;

    /**
     * Constructs a new instance of Frequency Encoder preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     * @param handledIndices Handled indices.
     */
    public TargetEncoderPreprocessor(TargetEncodingMeta[] targetCounters,
                                     Preprocessor<K, V> basePreprocessor, Set<Integer> handledIndices) {
        super(null, basePreprocessor, handledIndices);
        this.targetCounters = targetCounters;
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
                if (targetCounters[i].getCategoryMean().containsKey(tmpObj.toString())) {
                    res[i] = targetCounters[i].getCategoryMean().get(tmpObj.toString());
                } else {
                    res[i] = targetCounters[i].getGlobalMean();
                }
            } else
                res[i] = (double)tmpObj;
        }

        return new LabeledVector(VectorUtils.of(res), tmp.label());
    }
}
