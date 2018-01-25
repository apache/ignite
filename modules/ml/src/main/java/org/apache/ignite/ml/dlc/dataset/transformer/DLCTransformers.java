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

package org.apache.ignite.ml.dlc.dataset.transformer;

import java.io.Serializable;
import org.apache.ignite.ml.dlc.dataset.DLCDataset;
import org.apache.ignite.ml.dlc.dataset.DLCLabeledDataset;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Aggregator which allows to find desired transformer from one partition data type to another. This class doesn't
 * introduce a new functionality, but helps to work efficiently with existing transformers.
 */
public class DLCTransformers {
    /**
     * Creates a new instance of transformer which transforms upstream data into the {@link DLCDataset} using the
     * specified feature extractor.
     *
     * @param featureExtractor feature extractor
     * @param features number of features
     * @param <K> type of an upstream value key
     * @param <V> type of an upstream value
     * @param <Q> type of replicated data of a partition
     * @return transformer
     */
    public static <K, V, Q extends Serializable> UpstreamToDatasetTransformer<K, V, Q> upstreamToDataset(
        IgniteBiFunction<K, V, double[]> featureExtractor, int features) {
        return new UpstreamToDatasetTransformer<>(featureExtractor, features);
    }

    /**
     * Creates a new instance of transformer which transforms upstream data into the {@link DLCLabeledDataset} using the
     * specified feature and label extractors.
     *
     * @param featureExtractor feature extractor
     * @param lbExtractor label extractor
     * @param features number of features
     * @param <K> type of an upstream value key
     * @param <V> type of an upstream value
     * @return transformer
     */
    public static <K, V, Q extends Serializable> UpstreamToLabeledDatasetTransformer<K, V, Q> upstreamToLabeledDataset(
        IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor, int features) {
        return new UpstreamToLabeledDatasetTransformer<>(featureExtractor, lbExtractor, features);
    }
}
