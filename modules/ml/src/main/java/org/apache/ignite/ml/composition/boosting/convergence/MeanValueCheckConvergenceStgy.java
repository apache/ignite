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

package org.apache.ignite.ml.composition.boosting.convergence;

import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 *
 *
 * @param <K>
 * @param <V>
 */
public class MeanValueCheckConvergenceStgy<K,V> extends ConvergenceCheckStrategy<K,V> {
    /** Serial version uid. */
    private static final long serialVersionUID = 8534776439755210864L;

    /**
     * Creates an intance of MeanValueCheckConvergenceStgy.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param lossGradient Loss gradient.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     */
    public MeanValueCheckConvergenceStgy(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor,
        double precision) {

        super(sampleSize, externalLbToInternalMapping, lossGradient, datasetBuilder, featureExtractor, lbExtractor, precision);
    }

}
