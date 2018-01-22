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

package org.apache.ignite.ml.dlearn.context.transformer.cache;

import java.util.ArrayList;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnContextFactory;
import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnPartition;
import org.apache.ignite.ml.dlearn.dataset.DLearnLabeledDataset;
import org.apache.ignite.ml.dlearn.dataset.part.DLearnLabeledDatasetPartition;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Creates a transformer which accepts cache learning context (produced by {@link CacheDLearnContextFactory}) and
 * constructs {@link DLearnLabeledDataset}.
 *
 * @param <K> type of keys in cache learning context
 * @param <V> type of values in cache learning context
 * @param <L> type of label
 */
public class CacheLabeledDatasetDLearnPartitionTransformer<K, V, L>
    implements DLearnContextTransformer<CacheDLearnPartition<K, V>, DLearnLabeledDatasetPartition<L>,
    DLearnLabeledDataset<L>> {
    /** */
    private static final long serialVersionUID = 3479218902890029731L;

    /** Feature extractor. */
    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    /** Label extractor. */
    private final IgniteBiFunction<K, V, L> lbExtractor;

    /**
     * Constructs a new instance of cache to labeled dataset partition transformer.
     *
     * @param featureExtractor feature extractor
     * @param lbExtractor label extractor
     */
    public CacheLabeledDatasetDLearnPartitionTransformer(
        IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void transform(CacheDLearnPartition<K, V> oldPart, DLearnLabeledDatasetPartition<L> newPart) {
        List<Cache.Entry<K, V>> partData = queryPartDataIntoList(oldPart);

        int m = partData.size(), n = 0;
        double[] features = null;
        L[] labels = (L[])new Object[m];

        for (int i = 0; i < partData.size(); i++) {
            Cache.Entry<K, V> entry = partData.get(i);
            double[] rowFeatures = featureExtractor.apply(entry.getKey(), entry.getValue());
            labels[i] = lbExtractor.apply(entry.getKey(), entry.getValue());

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
        newPart.setLabels(labels);
    }

    /** {@inheritDoc} */
    @Override public DLearnLabeledDataset<L> wrapContext(DLearnContext<DLearnLabeledDatasetPartition<L>> ctx) {
        return new DLearnLabeledDataset<>(ctx);
    }

    /** {@inheritDoc} */
    @Override public DLearnLabeledDatasetPartition<L> createPartition(DLearnPartitionStorage storage) {
        return new DLearnLabeledDatasetPartition<>(storage);
    }

    /**
     * Retrieves local partition data from the cache via {@link ScanQuery} and collects it into list.
     *
     * @param oldPart partition
     * @return list of cache entries
     */
    private List<Cache.Entry<K, V>> queryPartDataIntoList(CacheDLearnPartition<K, V> oldPart) {
        List<Cache.Entry<K, V>> partData = new ArrayList<>();
        for (Cache.Entry<K, V> entry : queryPartData(oldPart))
            partData.add(entry);
        return partData;
    }

    /**
     * Retrieves local partition data from the cache via {@link ScanQuery} and returns cursor.
     *
     * @param oldPart partition
     * @return cursor
     */
    private Iterable<Cache.Entry<K, V>> queryPartData(CacheDLearnPartition<K, V> oldPart) {
        Ignite ignite = Ignition.localIgnite();
        IgniteCache<K, V> upstreamCache = ignite.cache(oldPart.getUpstreamCacheName());

        ScanQuery<K, V> qry = new ScanQuery<>();
        qry.setPartition(oldPart.getPart());

        return upstreamCache.query(qry);
    }
}
