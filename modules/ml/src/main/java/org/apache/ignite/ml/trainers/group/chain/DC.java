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
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;

public class DC {
    public static
    <L extends HasTrainingUUID, K, V, I, C extends HasCacheContext<GroupTrainerCacheKey<K>, V> & HasLocalContext<L>> DistributedTrainerWorkersChain<L, K, V, I, C, I> create() {
        return (input, context) -> input;
    }

    public static
    <L extends HasTrainingUUID, K, V, G, I, C extends HasCacheContext<GroupTrainerCacheKey<K>, V> & HasLocalContext<L>, O extends Serializable> DistributedTrainerWorkersChain<L, K, V, I, C, O> create(RemoteStep<L, K, V, G, I, O> step) {
        DistributedTrainerWorkersChain<L, K, V, I, C, I> chain = create();
        return chain.thenDistributed(step);
    }

    public static
    <L extends HasTrainingUUID, V, K, I, C extends HasCacheContext<GroupTrainerCacheKey<K>, V> & HasLocalContext<L>> DistributedTrainerWorkersChain<L, K, V, I, C, I> fromLocal() {
        return (input, context) -> input;
    }
}
