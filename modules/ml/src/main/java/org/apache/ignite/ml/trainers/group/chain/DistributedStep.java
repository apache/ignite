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

package org.apache.ignite.ml.trainers.group.chain;

import java.io.Serializable;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.ResultAndUpdates;

/**
 * Class encapsulating logic of distributed step in {@link ComputationsChain}.
 *
 * @param <L> Local context.
 * @param <K> Type of keys of cache used for group training.
 * @param <V> Type of values of cache used for group training.
 * @param <C> Context used by worker.
 * @param <I> Type of input to this step.
 * @param <O> Type of output of this step.
 */
public interface DistributedStep<L, K, V, C, I, O extends Serializable> {
    /**
     * Extracts context used by worker.
     *
     * @param input Input.
     * @param locCtx Local context.
     * @return Context used by worker.
     */
    C extractRemoteContext(I input, L locCtx);

    /**
     * Function applied to each cache entry specified by keys.
     *
     * @param entryAndCtx Cache entry and context.
     * @return ResultAndUpdates object.
     */
    ResultAndUpdates<O> worker(EntryAndContext<K, V, C> entryAndCtx);

    /**
     * Get of keys for worker.
     *
     * @param input Input to this step.
     * @param locCtx Local context.
     * @return Keys for worker.
     */
    Stream<GroupTrainerCacheKey<K>> keys(I input, L locCtx);

    /**
     * Function used to reduce results returned by worker.
     *
     * @param arg1 First argument to reducer.
     * @param arg2 Second argument to reducer.
     * @return Result of reducing.
     */
    O reduce(O arg1, O arg2);

    /**
     * Identity for reduce.
     * @return Identity for reduce.
     */
    O identity();
}
