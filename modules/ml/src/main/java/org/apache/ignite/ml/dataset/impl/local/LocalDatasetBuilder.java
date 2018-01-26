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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.PartitionUpstreamEntry;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class LocalDatasetBuilder<K, V, C extends Serializable, D extends AutoCloseable>
    implements DatasetBuilder<C, D> {

    private final Map<K, V> upstreamMap;

    private final int partitions;

    private final PartitionContextBuilder<K, V, C> partCtxBuilder;

    private final PartitionDataBuilder<K, V, C, D> partDataBuilder;

    public LocalDatasetBuilder(Map<K, V> upstreamMap, int partitions,
        PartitionContextBuilder<K, V, C> partCtxBuilder, PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        this.upstreamMap = upstreamMap;
        this.partitions = partitions;
        this.partCtxBuilder = partCtxBuilder;
        this.partDataBuilder = partDataBuilder;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Dataset<C, D> build() {
        List<C> ctx = new ArrayList<>();
        List<D> data = new ArrayList<>();

        int partSize = upstreamMap.size() / partitions;

        Iterator<K> firstKeysIter = upstreamMap.keySet().iterator();
        Iterator<K> secondKeysIter = upstreamMap.keySet().iterator();
        for (int part = 0; part < partitions; part++) {
            int cnt = Math.min((part + 1) * partSize, upstreamMap.size()) - part * partSize;

            C c = partCtxBuilder.build(
                new IteratorWindow<>(firstKeysIter, k -> new PartitionUpstreamEntry<>(k, upstreamMap.get(k)), cnt),
                cnt
            );

            D d = partDataBuilder.build(
                new IteratorWindow<>(secondKeysIter, k -> new PartitionUpstreamEntry<>(k, upstreamMap.get(k)), cnt),
                cnt,
                c
            );

            ctx.add(c);
            data.add(d);
        }

        return new LocalDataset<>(ctx, data);
    }

    private static class IteratorWindow<K, T> implements Iterator<T> {

        private final Iterator<K> delegate;

        private final IgniteFunction<K, T> map;

        private final int cnt;

        private int ptr;

        public IteratorWindow(Iterator<K> delegate, IgniteFunction<K, T> map, int cnt) {
            this.delegate = delegate;
            this.map = map;
            this.cnt = cnt;
        }

        @Override public boolean hasNext() {
            return delegate.hasNext() && ptr < cnt;
        }

        @Override public T next() {
            ptr++;
            return map.apply(delegate.next());
        }
    }
}
