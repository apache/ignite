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

package org.apache.ignite.ml.trainers.group;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;

/**
 * {@link BaseLocalProcessorJob} specified to entry processing.
 *
 * @param <K> Type of cache used for group training.
 * @param <V> Type of values used for group training.
 * @param <C> Type of context.
 * @param <R> Type of result returned by worker.
 */
public class LocalEntriesProcessorJob<K, V, C, R extends Serializable> extends BaseLocalProcessorJob<K, V, EntryAndContext<K, V, C>, R> {
    /**
     * Supplier of context for worker.
     */
    private final IgniteSupplier<C> ctxSupplier;

    /**
     * Construct an instance of this class.
     *
     * @param ctxSupplier Supplier for context for worker.
     * @param worker Worker.
     * @param keySupplier Supplier of keys.
     * @param reducer Reducer.
     * @param identity Identity for reducer.
     * @param trainingUUID UUID for training.
     * @param cacheName Name of cache used for training.
     */
    public LocalEntriesProcessorJob(IgniteSupplier<C> ctxSupplier,
        IgniteFunction<EntryAndContext<K, V, C>, ResultAndUpdates<R>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier,
        IgniteBinaryOperator<R> reducer, R identity,
        UUID trainingUUID, String cacheName) {
        super(worker, keySupplier, reducer, identity, trainingUUID, cacheName);
        this.ctxSupplier = ctxSupplier;
    }

    /** {@inheritDoc} */
    @Override protected Stream<EntryAndContext<K, V, C>> toProcess() {
        C ctx = ctxSupplier.get();

        return selectLocalEntries().
            //parallel().
            map(e -> new EntryAndContext<>(e, ctx));
    }

    /**
     * Select entries for processing by worker.
     *
     * @return Entries for processing by worker.
     */
    private Stream<Map.Entry<GroupTrainerCacheKey<K>, V>> selectLocalEntries() {
        Set<GroupTrainerCacheKey<K>> keys = keySupplier.get().
            filter(k -> Objects.requireNonNull(affinity().mapKeyToNode(k)).isLocal()).
            filter(k -> k.trainingUUID().equals(trainingUUID)).
            collect(Collectors.toSet());

        return cache().getAll(keys).entrySet().stream();
    }
}
