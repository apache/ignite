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
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.KeyAndContext;

/**
 * Task for processing entries of cache used for training.
 *
 * @param <K> Type of cache keys of cache used for training.
 * @param <C> Type of context (common part of data needed for computation).
 * @param <R> Type of computation result.
 */
public class GroupTrainerKeysProcessorTask<K, C, R extends Serializable> extends GroupTrainerBaseProcessorTask<K, Object, C, KeyAndContext<K, C>, R> {
    /**
     * Construct instance of this class with specified parameters.
     *
     * @param trainingUUID UUID of training.
     * @param ctxSupplier Context supplier.
     * @param worker Function calculated on each of specified keys.
     * @param keysSupplier Supplier of keys on which computations should be done.
     * @param reducer Reducer used for reducing results of computation performed on each of specified keys.
     * @param cacheName Name of cache on which training is done.
     * @param ignite Ignite instance.
     */
    public GroupTrainerKeysProcessorTask(UUID trainingUUID,
        IgniteSupplier<C> ctxSupplier,
        IgniteFunction<KeyAndContext<K, C>, ResultAndUpdates<R>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier,
        IgniteFunction<List<R>, R> reducer,
        String cacheName,
        Ignite ignite) {
        super(trainingUUID, ctxSupplier, worker, keysSupplier, reducer, cacheName, ignite);
    }

    /** {@inheritDoc} */
    @Override protected BaseLocalProcessorJob<K, Object, KeyAndContext<K, C>, R> createJob() {
        return new LocalKeysProcessorJob<>(ctxSupplier, worker, keysSupplier, reducer, trainingUUID, cacheName);
    }
}
