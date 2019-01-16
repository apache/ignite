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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformer;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.util.Utils;

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

    /** Upstream transformers. */
    private final UpstreamTransformerBuilder upstreamTransformerBuilder;

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
     * @param upstreamTransformerBuilder Builder of upstream transformer.
     */
    public LocalDatasetBuilder(Map<K, V> upstreamMap,
        IgniteBiPredicate<K, V> filter,
        int partitions,
        UpstreamTransformerBuilder upstreamTransformerBuilder) {
        this.upstreamMap = upstreamMap;
        this.filter = filter;
        this.partitions = partitions;
        this.upstreamTransformerBuilder = upstreamTransformerBuilder;
    }

    /**
     * Constructs a new instance of local dataset builder that makes {@link LocalDataset}.
     *
     * @param upstreamMap {@code Map} with upstream data.
     * @param filter Filter for {@code upstream} data.
     * @param partitions Number of partitions.
     */
    public LocalDatasetBuilder(Map<K, V> upstreamMap,
        IgniteBiPredicate<K, V> filter,
        int partitions) {
        this(upstreamMap, filter, partitions, UpstreamTransformerBuilder.identity());
    }

    /** {@inheritDoc} */
    @Override public <C extends Serializable, D extends AutoCloseable> LocalDataset<C, D> build(
        LearningEnvironmentBuilder envBuilder,
        PartitionContextBuilder<K, V, C> partCtxBuilder, PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        List<C> ctxList = new ArrayList<>();
        List<D> dataList = new ArrayList<>();

        List<UpstreamEntry<K, V>> entriesList = new ArrayList<>();

        upstreamMap
            .entrySet()
            .stream()
            .filter(en -> filter.apply(en.getKey(), en.getValue()))
            .map(en -> new UpstreamEntry<>(en.getKey(), en.getValue()))
            .forEach(entriesList::add);

        int partSize = Math.max(1, entriesList.size() / partitions);

        Iterator<UpstreamEntry<K, V>> firstKeysIter = entriesList.iterator();
        Iterator<UpstreamEntry<K, V>> secondKeysIter = entriesList.iterator();
        Iterator<UpstreamEntry<K, V>> thirdKeysIter = entriesList.iterator();

        int ptr = 0;

        List<LearningEnvironment> envs = IntStream.range(0, partitions).boxed().map(envBuilder::buildForWorker)
            .collect(Collectors.toList());

        for (int part = 0; part < partitions; part++) {
            int cntBeforeTransform =
                part == partitions - 1 ? entriesList.size() - ptr : Math.min(partSize, entriesList.size() - ptr);
            LearningEnvironment env = envs.get(part);
            UpstreamTransformer transformer1 = upstreamTransformerBuilder.build(env);
            UpstreamTransformer transformer2 = Utils.copy(transformer1);
            UpstreamTransformer transformer3 = Utils.copy(transformer1);

            int cnt = (int)transformer1.transform(Utils.asStream(new IteratorWindow<>(thirdKeysIter, k -> k, cntBeforeTransform))).count();

            Iterator<UpstreamEntry> iter =
                transformer2.transform(Utils.asStream(new IteratorWindow<>(firstKeysIter, k -> k, cntBeforeTransform)).map(x -> (UpstreamEntry)x)).iterator();
            Iterator<UpstreamEntry<K, V>> convertedBack = Utils.asStream(iter).map(x -> (UpstreamEntry<K, V>)x).iterator();

            C ctx = cntBeforeTransform > 0 ? partCtxBuilder.build(env, convertedBack, cnt) : null;

            Iterator<UpstreamEntry> iter1 = transformer3.transform(
                    Utils.asStream(new IteratorWindow<>(secondKeysIter, k -> k, cntBeforeTransform))).iterator();

            Iterator<UpstreamEntry<K, V>> convertedBack1 = Utils.asStream(iter1).map(x -> (UpstreamEntry<K, V>)x).iterator();

            D data = cntBeforeTransform > 0 ? partDataBuilder.build(
                env,
                convertedBack1,
                cnt,
                ctx
            ) : null;

            ctxList.add(ctx);
            dataList.add(data);

            ptr += cntBeforeTransform;
        }

        return new LocalDataset<>(envs, ctxList, dataList);
    }

    /** {@inheritDoc} */
    @Override public DatasetBuilder<K, V> withUpstreamTransformer(UpstreamTransformerBuilder builder) {
        return new LocalDatasetBuilder<>(upstreamMap, filter, partitions, upstreamTransformerBuilder.andThen(builder));
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
