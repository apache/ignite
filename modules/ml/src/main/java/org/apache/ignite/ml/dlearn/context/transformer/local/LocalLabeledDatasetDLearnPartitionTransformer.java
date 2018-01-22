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

package org.apache.ignite.ml.dlearn.context.transformer.local;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnContextFactory;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnPartition;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.dlearn.dataset.DLearnLabeledDataset;
import org.apache.ignite.ml.dlearn.dataset.part.DLearnLabeledDatasetPartition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Creates a transformer which accepts cache learning context (produced by {@link LocalDLearnContextFactory}) and
 * constructs {@link DLearnLabeledDataset}.
 *
 * @param <K> type of keys in local learning context
 * @param <V> type of values in local learning context
 * @param <L> type of label
 */
public class LocalLabeledDatasetDLearnPartitionTransformer<K, V, L>
    implements DLearnContextTransformer<LocalDLearnPartition<K, V>, DLearnLabeledDatasetPartition<L>,
    DLearnLabeledDataset<L>> {
    /** */
    private static final long serialVersionUID = -8438445094768312331L;

    /** Feature extractor. */
    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    /** Label extractor. */
    private final IgniteBiFunction<K, V, L> lbExtractor;

    /**
     * Creates new instance of local to labeled dataset transformer.
     *
     * @param featureExtractor feature extractor
     * @param lbExtractor label extractor
     */
    public LocalLabeledDatasetDLearnPartitionTransformer(IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void transform(LocalDLearnPartition<K, V> oldPart, DLearnLabeledDatasetPartition<L> newPart) {
        Map<K, V> partData = oldPart.getPartData();

        if (partData != null && !partData.isEmpty()) {
            double[] features = null;
            int m = partData.size(), n = 0;

            List<K> keys = new ArrayList<>(partData.keySet());

            for (int i = 0; i < partData.size(); i++) {
                K key = keys.get(i);
                double[] rowFeatures = featureExtractor.apply(key, partData.get(key));

                if (i == 0) {
                    n = rowFeatures.length;
                    features = new double[m * n];
                }

                if (rowFeatures.length != n)
                    throw new IllegalStateException();

                for (int j = 0; j < rowFeatures.length; j++)
                    features[j * m + i] = rowFeatures[j];
            }

            newPart.setFeatures(features);
            newPart.setRows(m);

            L[] labels = (L[])new Object[partData.size()];

            for (int i = 0; i < partData.size(); i++) {
                K key = keys.get(i);
                labels[i] = lbExtractor.apply(key, partData.get(key));
            }

            newPart.setLabels(labels);
        }
    }

    /** {@inheritDoc} */
    @Override public DLearnLabeledDataset<L> wrapContext(DLearnContext<DLearnLabeledDatasetPartition<L>> ctx) {
        return new DLearnLabeledDataset<>(ctx);
    }

    /** {@inheritDoc} */
    @Override public DLearnLabeledDatasetPartition<L> createPartition(DLearnPartitionStorage storage) {
        return new DLearnLabeledDatasetPartition<>(storage);
    }
}
