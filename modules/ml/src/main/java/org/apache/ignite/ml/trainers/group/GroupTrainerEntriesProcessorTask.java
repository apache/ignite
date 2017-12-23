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
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;

/**
 * Task for processing entries of cache used for training.
 *
 * @param <K> Type of cache keys of cache used for training.
 * @param <V> Type of cache values of cache used for training.
 * @param <C> Type of context (common part of data needed for computation).
 * @param <R> Type of computation result.
 */
public class GroupTrainerEntriesProcessorTask<K, V, C, R extends Serializable> extends GroupTrainerBaseProcessorTask<K, V, C, EntryAndContext<K, V, C>, R> {
    /**
     * Construct instance of this class with given parameters.
     *
     * @param trainingUUID UUID of training.
     * @param ctxSupplier Supplier of context.
     * @param worker Function calculated on each of specified keys.
     * @param keysSupplier Supplier of keys on which training is done.
     * @param reducer Reducer used for reducing results of computation performed on each of specified keys.
     * @param identity Identity for reducer.
     * @param cacheName Name of cache on which training is done.
     * @param ignite Ignite instance.
     */
    public GroupTrainerEntriesProcessorTask(UUID trainingUUID,
        IgniteSupplier<C> ctxSupplier,
        IgniteFunction<EntryAndContext<K, V, C>, ResultAndUpdates<R>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier,
        IgniteBinaryOperator<R> reducer, R identity,
        String cacheName,
        Ignite ignite) {
        super(trainingUUID, ctxSupplier, worker, keysSupplier, reducer, identity, cacheName, ignite);
    }

    /** {@inheritDoc} */
    @Override protected BaseLocalProcessorJob<K, V, EntryAndContext<K, V, C>, R> createJob() {
        return new LocalEntriesProcessorJob<>(ctxSupplier, worker, keysSupplier, identity, reducer, trainingUUID, cacheName);
    }
}
