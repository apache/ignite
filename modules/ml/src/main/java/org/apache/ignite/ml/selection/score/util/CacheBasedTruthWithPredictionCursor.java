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

package org.apache.ignite.ml.selection.score.util;

import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.selection.score.TruthWithPrediction;
import org.jetbrains.annotations.NotNull;

public class CacheBasedTruthWithPredictionCursor<L, K, V> implements TruthWithPredictionCursor<L> {

    private final QueryCursor<Cache.Entry<K, V>> cursor;

    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    private final IgniteBiFunction<K, V, L> lbExtractor;

    private final Model<double[], L> mdl;

    public CacheBasedTruthWithPredictionCursor(IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor,
        Model<double[], L> mdl) {
        this.cursor = query(upstreamCache, filter);
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.mdl = mdl;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cursor.close();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<TruthWithPrediction<L>> iterator() {
        return new TruthWithPredictionIterator(cursor.iterator());
    }

    private QueryCursor<Cache.Entry<K, V>> query(IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter) {
        ScanQuery<K, V> qry = new ScanQuery<>();
        qry.setFilter(filter);

        return upstreamCache.query(qry);
    }

    private class TruthWithPredictionIterator implements Iterator<TruthWithPrediction<L>> {

        private final Iterator<Cache.Entry<K, V>> iter;

        public TruthWithPredictionIterator(Iterator<Cache.Entry<K, V>> iter) {
            this.iter = iter;
        }

        @Override public boolean hasNext() {
            return iter.hasNext();
        }

        @Override public TruthWithPrediction<L> next() {
            Cache.Entry<K, V> entry = iter.next();

            double[] features = featureExtractor.apply(entry.getKey(), entry.getValue());
            L lb = lbExtractor.apply(entry.getKey(), entry.getValue());

            return new TruthWithPrediction<>(lb, mdl.apply(features));
        }
    }
}
