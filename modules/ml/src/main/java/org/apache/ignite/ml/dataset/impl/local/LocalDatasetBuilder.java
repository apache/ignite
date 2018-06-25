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

package org.apache.ignite.ml.dataset.impl.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * A dataset builder that makes {@link LocalDataset}. Encapsulate logic of building local dataset such as allocation
 * required data structures and initialization of {@code context} part of partitions.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class LocalDatasetBuilder<K, V> implements DatasetBuilder<K, V> {
    /** {@code Map} with upstream data. */
    private final Map<K, V> upstreamMap;

    /** Number of partitions. */
    private final int partitions;

    /** Filter for {@code upstream} data. */
    private final IgniteBiPredicate<K, V> filter;

    /**
     * Constructs a new instance of local dataset builder that makes {@link LocalDataset} with default predicate that
     * passes all upstream entries to dataset.
     *
     * @param upstreamMap {@code Map} with upstream data.
     * @param partitions Number of partitions.
     */
    public LocalDatasetBuilder(Map<K, V> upstreamMap, int partitions) {
        this(upstreamMap, (a, b) -> true, partitions);
    }

    /**
     * Constructs a new instance of local dataset builder that makes {@link LocalDataset}.
     *
     * @param upstreamMap {@code Map} with upstream data.
     * @param filter Filter for {@code upstream} data.
     * @param partitions Number of partitions.
     */
    public LocalDatasetBuilder(Map<K, V> upstreamMap, IgniteBiPredicate<K, V> filter, int partitions) {
        this.upstreamMap = upstreamMap;
        this.filter = filter;
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <C extends Serializable, D extends AutoCloseable> LocalDataset<C, D> build(
        PartitionContextBuilder<K, V, C> partCtxBuilder, PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        List<C> ctxList = new ArrayList<>();
        List<D> dataList = new ArrayList<>();

        Map<K, V> filteredMap = new HashMap<>();
        upstreamMap.forEach((key, val) -> {
            if (filter.apply(key, val))
                filteredMap.put(key, val);
        });

        int partSize = Math.max(1, filteredMap.size() / partitions);

        Iterator<K> firstKeysIter = filteredMap.keySet().iterator();
        Iterator<K> secondKeysIter = filteredMap.keySet().iterator();

        int ptr = 0;
        for (int part = 0; part < partitions; part++) {
            int cnt = part == partitions - 1 ? filteredMap.size() - ptr : Math.min(partSize, filteredMap.size() - ptr);

            C ctx = cnt > 0 ? partCtxBuilder.build(
                new IteratorWindow<>(firstKeysIter, k -> new UpstreamEntry<>(k, filteredMap.get(k)), cnt),
                cnt
            ) : null;

            D data = cnt > 0 ? partDataBuilder.build(
                new IteratorWindow<>(secondKeysIter, k -> new UpstreamEntry<>(k, filteredMap.get(k)), cnt),
                cnt,
                ctx
            ) : null;

            ctxList.add(ctx);
            dataList.add(data);

            ptr += cnt;
        }

        return new LocalDataset<>(ctxList, dataList);
    }

    /** {@inheritDoc} */
    @Override public DatasetBuilder<K, V> withFilter(IgniteBiPredicate<K, V> filterToAdd) {
        return new LocalDatasetBuilder<>(upstreamMap,
            (e1, e2) -> filter.apply(e1, e2) && filterToAdd.apply(e1, e2), partitions);
    }

    /**
     * Utils class that wraps iterator so that it produces only specified number of entries and allows to transform
     * entries from one type to another.
     *
     * @param <K> Initial type of entries.
     * @param <T> Target type of entries.
     */
    private static class IteratorWindow<K, T> implements Iterator<T> {
        /** Delegate iterator. */
        private final Iterator<K> delegate;

        /** Transformer that transforms entries from one type to another. */
        private final IgniteFunction<K, T> map;

        /** Count of entries to produce. */
        private final int cnt;

        /** Number of already produced entries. */
        private int ptr;

        /**
         * Constructs a new instance of iterator window wrapper.
         *
         * @param delegate Delegate iterator.
         * @param map Transformer that transforms entries from one type to another.
         * @param cnt Count of entries to produce.
         */
        IteratorWindow(Iterator<K> delegate, IgniteFunction<K, T> map, int cnt) {
            this.delegate = delegate;
            this.map = map;
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return delegate.hasNext() && ptr < cnt;
        }

        /** {@inheritDoc} */
        @Override public T next() {
            ++ptr;

            return map.apply(delegate.next());
        }
    }
}
