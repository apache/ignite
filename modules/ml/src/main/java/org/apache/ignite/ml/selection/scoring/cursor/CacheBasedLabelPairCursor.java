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

package org.apache.ignite.ml.selection.scoring.cursor;

import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.selection.scoring.LabelPair;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;

/**
 * Truth with prediction cursor based on a data stored in Ignite cache.
 *
 * @param <L> Type of a label (truth or prediction).
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class CacheBasedLabelPairCursor<L, K, V> implements LabelPairCursor<L> {
    /** Query cursor. */
    private final QueryCursor<Cache.Entry<K, V>> cursor;

    /** Preprocessor. */
    private final Preprocessor<K, V> preprocessor;

    /** Model for inference. */
    private final IgniteModel<Vector, L> mdl;

    /**
     * Constructs a new instance of cache based truth with prediction cursor.
     *
     * @param upstreamCache Ignite cache with {@code upstream} data.
     * @param filter Filter for {@code upstream} data.
     * @param preprocessor Preprocessor.
     * @param mdl Model for inference.
     */
    public CacheBasedLabelPairCursor(IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter,
                                     Preprocessor<K, V> preprocessor,
                                     IgniteModel<Vector, L> mdl) {
        cursor = query(upstreamCache, filter);
        this.preprocessor = preprocessor;
        this.mdl = mdl;
    }

    /**
     * Constructs a new instance of cache based truth with prediction cursor.
     *
     * @param upstreamCache Ignite cache with {@code upstream} data.
     * @param preprocessor Preprocessor.
     * @param mdl Model for inference.
     */
    public CacheBasedLabelPairCursor(IgniteCache<K, V> upstreamCache,
                                     Preprocessor<K, V> preprocessor,
                                     IgniteModel<Vector, L> mdl) {
        cursor = query(upstreamCache);
        this.preprocessor = preprocessor;
        this.mdl = mdl;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cursor.close();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<LabelPair<L>> iterator() {
        return new TruthWithPredictionIterator(cursor.iterator());
    }

    /**
     * Queries the specified cache using the specified filter.
     *
     * @param upstreamCache Ignite cache with {@code upstream} data.
     * @param filter Filter for {@code upstream} data. If {@code null} then all entries will be returned.
     * @return Query cursor.
     */
    private QueryCursor<Cache.Entry<K, V>> query(IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter) {
        ScanQuery<K, V> qry = new ScanQuery<>();

        if (filter != null) // This section was added to keep code correct of qry.setFilter(null) behaviour will changed.
            qry.setFilter(filter);

        return upstreamCache.query(qry);
    }

    /**
     * Queries the specified cache using the specified filter.
     *
     * @param upstreamCache Ignite cache with {@code upstream} data.
     * @return Query cursor.
     */
    private QueryCursor<Cache.Entry<K, V>> query(IgniteCache<K, V> upstreamCache) {
        ScanQuery<K, V> qry = new ScanQuery<>();

        return upstreamCache.query(qry);
    }

    /**
     * Util iterator that makes predictions using the model.
     */
    private class TruthWithPredictionIterator implements Iterator<LabelPair<L>> {
        /** Base iterator. */
        private final Iterator<Cache.Entry<K, V>> iter;

        /**
         * Constructs a new instance of truth with prediction iterator.
         *
         * @param iter Base iterator.
         */
        public TruthWithPredictionIterator(Iterator<Cache.Entry<K, V>> iter) {
            this.iter = iter;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public LabelPair<L> next() {
            Cache.Entry<K, V> entry = iter.next();

            LabeledVector<L> lv = preprocessor.apply(entry.getKey(), entry.getValue());

            return new LabelPair<>(lv.label(), mdl.predict(lv.features()));
        }
    }
}
