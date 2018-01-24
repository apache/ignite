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

import javax.cache.Cache;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Aggregator which allows to find desired transformer from one partition data type to another. This class doesn't
 * introduce a new functionality, but helps to work efficiently with existing transformers.
 */
public class DLCTransformers {

    public static <K, V> UpstreamToDatasetTransformer<K, V> upstreamToDataset(
        IgniteFunction<Cache.Entry<K, V>, double[]> featureExtractor, int features) {
        return new UpstreamToDatasetTransformer<>(featureExtractor, features);
    }

    public static <K, V> UpstreamToLabeledDatasetTransformer<K, V> upstreamToLabeledDataset(
        IgniteFunction<Cache.Entry<K, V>, double[]> featureExtractor,
        IgniteFunction<Cache.Entry<K, V>, Double> lbExtractor, int features) {
        return new UpstreamToLabeledDatasetTransformer<>(featureExtractor, lbExtractor, features);
    }
}
