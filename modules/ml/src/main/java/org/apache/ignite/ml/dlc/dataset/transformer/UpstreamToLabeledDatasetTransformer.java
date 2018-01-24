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
import javax.cache.Cache;
import org.apache.ignite.ml.dlc.DLCPartitionRecoverableDataTransformer;
import org.apache.ignite.ml.dlc.dataset.part.recoverable.DLCLabeledDatasetPartitionRecoverable;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Transforms upstream data into labeled dataset recoverable part of partition using specified feature extractor and
 * label extractor.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class UpstreamToLabeledDatasetTransformer<K, V> implements DLCPartitionRecoverableDataTransformer<K, V, Serializable, DLCLabeledDatasetPartitionRecoverable> {
    /** */
    private static final long serialVersionUID = -1224768715207401297L;

    /** Feature extractor. */
    private final IgniteFunction<Cache.Entry<K, V>, double[]> featureExtractor;

    /** Label extractor. */
    private final IgniteFunction<Cache.Entry<K, V>, Double> lbExtractor;

    /** Number of features. */
    private final int features;

    /**
     * Constructs a new instance of transformer.
     *
     * @param featureExtractor feature extractor
     * @param lbExtractor label extractor
     * @param features number of features
     */
    protected UpstreamToLabeledDatasetTransformer(
        IgniteFunction<Cache.Entry<K, V>, double[]> featureExtractor,
        IgniteFunction<Cache.Entry<K, V>, Double> lbExtractor, int features) {
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.features = features;
    }

    /** {@inheritDoc} */
    @Override public DLCLabeledDatasetPartitionRecoverable apply(Iterable<Cache.Entry<K, V>> entries, Long aLong,
        Serializable ser) {
        double[][] features = new double[Math.toIntExact(aLong)][];
        double[] labels = new double[Math.toIntExact(aLong)];
        int ptr = 0;
        for (Cache.Entry<K, V> e : entries) {
            features[ptr] = featureExtractor.apply(e);
            labels[ptr] = lbExtractor.apply(e);
            ptr++;
        }
        return new DLCLabeledDatasetPartitionRecoverable(features, labels);
    }
}
