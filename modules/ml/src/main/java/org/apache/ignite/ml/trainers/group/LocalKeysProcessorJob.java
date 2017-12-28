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
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.KeyAndContext;

/**
 * {@link BaseLocalProcessorJob} specified to keys processing.
 *
 * @param <K> Type of cache used for group training.
 * @param <V> Type of values used for group training.
 * @param <C> Type of context.
 * @param <R> Type of result returned by worker.
 */
public class LocalKeysProcessorJob<K, V, C, R extends Serializable> extends BaseLocalProcessorJob<K, V, KeyAndContext<K, C>, R> {
    /**
     * Supplier of worker context.
     */
    private final IgniteSupplier<C> ctxSupplier;

    /**
     * Construct instance of this class with given arguments.
     * @param worker Worker.
     * @param keySupplier Supplier of keys.
     * @param reducer Reducer.
     * @param identity Identity for reducer.
     * @param trainingUUID UUID of training.
     * @param cacheName Name of cache used for training.
     */
    public LocalKeysProcessorJob(IgniteSupplier<C> ctxSupplier,
        IgniteFunction<KeyAndContext<K, C>, ResultAndUpdates<R>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier,
        IgniteBinaryOperator<R> reducer, R identity,
        UUID trainingUUID, String cacheName) {
        super(worker, keySupplier, reducer, identity, trainingUUID, cacheName);
        this.ctxSupplier = ctxSupplier;
    }

    /** {@inheritDoc} */
    @Override protected Stream<KeyAndContext<K, C>> toProcess() {
        C ctx = ctxSupplier.get();

        return selectLocalKeys().map(k -> new KeyAndContext<>(k, ctx));
    }

    /**
     * Get subset of keys provided by keySupplier which are mapped to node on which code is executed.
     *
     * @return Subset of keys provided by keySupplier which are mapped to node on which code is executed.
     */
    private Stream<GroupTrainerCacheKey<K>> selectLocalKeys() {
        return keySupplier.get().
            filter(k -> Objects.requireNonNull(affinity().mapKeyToNode(k)).isLocal()).
            filter(k -> k.trainingUUID().equals(trainingUUID));
    }
}
