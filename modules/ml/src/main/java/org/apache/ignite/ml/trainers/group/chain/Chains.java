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

/**
 * Class containing methods creating {@link ComputationsChain}.
 */
public class Chains {
    /**
     * Create computation chain consisting of one returning its input as output.
     *
     * @param <L> Type of local context of created chain.
     * @param <K> Type of keys of cache used in computation chain.
     * @param <V> Type of values of cache used in computation chain.
     * @param <I> Type of input to computation chain.
     * @return Computation chain consisting of one returning its input as output.
     */
    public static
    <L extends HasTrainingUUID, K, V, I> ComputationsChain<L, K, V, I, I> create() {
        return (input, context) -> input;
    }

    /**
     * Create {@link ComputationsChain} from {@link DistributedEntryProcessingStep}.
     *
     * @param step Distributed chain step.
     * @param <L> Type of local context of created chain.
     * @param <K> Type of keys of cache used in computation chain.
     * @param <V> Type of values of cache used in computation chain.
     * @param <C> Type of context used by worker in {@link DistributedEntryProcessingStep}.
     * @param <I> Type of input to computation chain.
     * @param <O> Type of output of computation chain.
     * @return Computation created from {@link DistributedEntryProcessingStep}.
     */
    public static
    <L extends HasTrainingUUID, K, V, C, I, O extends Serializable> ComputationsChain<L, K, V, I, O> create(DistributedEntryProcessingStep<L, K, V, C, I, O> step) {
        ComputationsChain<L, K, V, I, I> chain = create();
        return chain.thenDistributedForEntries(step);
    }
}
