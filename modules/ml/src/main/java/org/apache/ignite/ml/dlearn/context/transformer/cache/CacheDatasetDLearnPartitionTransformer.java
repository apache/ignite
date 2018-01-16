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
import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnPartition;
import org.apache.ignite.ml.dlearn.part.DatasetDLeanPartition;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/** */
public class CacheDatasetDLearnPartitionTransformer<K, V> implements IgniteBiConsumer<CacheDLearnPartition<K, V>, DatasetDLeanPartition> {
    /** */
    private static final long serialVersionUID = -7398727071330763144L;

    /** */
    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    /** */
    public CacheDatasetDLearnPartitionTransformer(IgniteBiFunction<K, V, double[]> featureExtractor) {
        this.featureExtractor = featureExtractor;
    }

    /** */
    @Override public void accept(CacheDLearnPartition<K, V> oldPart, DatasetDLeanPartition newPart) {
        List<Cache.Entry<K, V>> partData = queryPartDataIntoList(oldPart);

        double[] features = null;
        int m = partData.size(), n = 0;
        for (int i = 0; i < partData.size(); i++) {
            Cache.Entry<K, V> entry = partData.get(i);
            double[] rowFeatures = featureExtractor.apply(entry.getKey(), entry.getValue());

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
    }

    /** */
    private List<Cache.Entry<K, V>> queryPartDataIntoList(CacheDLearnPartition<K, V> oldPart) {
        List<Cache.Entry<K, V>> partData = new ArrayList<>();
        for (Cache.Entry<K, V> entry : queryPartData(oldPart))
            partData.add(entry);
        return partData;
    }

    /** */
    private Iterable<Cache.Entry<K, V>> queryPartData(CacheDLearnPartition<K, V> oldPart) {
        Ignite ignite = Ignition.localIgnite();
        IgniteCache<K, V> upstreamCache = ignite.cache(oldPart.getUpstreamCacheName());

        ScanQuery<K, V> qry = new ScanQuery<>();
        qry.setLocal(true);
        qry.setPartition(oldPart.getPart());

        return upstreamCache.query(qry);
    }
}
